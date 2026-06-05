import gzip
import io
import json
import logging
import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone

import requests
from astropy.io import fits
from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.files.uploadedfile import SimpleUploadedFile


logger = logging.getLogger(__name__)


PUBLIC_UPLOAD_FILTER_CHOICES = [
    ('no', 'Auto'),
    ('2MASS/J', '2MASS/J'),
    ('2MASS/H', '2MASS/H'),
    ('2MASS/K', '2MASS/K'),
    ('2MASS/any', '2MASS/any'),
    ('GaiaSP/u', 'GaiaSP/u'),
    ('GaiaSP/g', 'GaiaSP/g'),
    ('GaiaSP/r', 'GaiaSP/r'),
    ('GaiaSP/i', 'GaiaSP/i'),
    ('GaiaSP/z', 'GaiaSP/z'),
    ('GaiaSP/U', 'GaiaSP/U'),
    ('GaiaSP/B', 'GaiaSP/B'),
    ('GaiaSP/V', 'GaiaSP/V'),
    ('GaiaSP/R', 'GaiaSP/R'),
    ('GaiaSP/I', 'GaiaSP/I'),
    ('GaiaSP/any', 'GaiaSP/any'),
    ('GaiaSP/ugriz', 'GaiaSP/ugriz'),
    ('GaiaSP/UBVRI', 'GaiaSP/UBVRI'),
    ('GaiaDR3/any', 'GaiaDR3/any'),
    ('GaiaDR3/G', 'GaiaDR3/G'),
    ('GaiaDR3/GBP', 'GaiaDR3/GBP'),
    ('GaiaDR3/GRP', 'GaiaDR3/GRP'),
]

SIMPLE_FITS_SUFFIXES = ('.fits', '.fit', '.fts', '.ftt', '.ftsc')
COMPRESSED_FITS_SUFFIXES = (
    '.fits.gz', '.fts.gz', '.ftt.gz', '.ftsc.gz', '.fit.gz',
    '.fits.fz', '.fts.fz', '.ftt.fz', '.ftsc.fz', '.fit.fz',
)
BHTOM2_UPLOAD_STATE_KEY = 'bhtom2_fits_upload'


class Bhtom2UploadError(Exception):
    pass


def is_supported_fits_filename(filename):
    lower_name = str(filename or '').lower()
    return lower_name.endswith(SIMPLE_FITS_SUFFIXES + COMPRESSED_FITS_SUFFIXES)


def _detect_fits_upload_format(file_name):
    lower_name = str(file_name or '').lower()
    if lower_name.endswith('.fz'):
        return 'fits.fz'
    if lower_name.endswith('.gz'):
        return 'fits.gz'
    return 'plain fits'


def _coerce_simple_fits_name(file_name):
    name = str(file_name or '').strip()
    lower_name = name.lower()
    if lower_name.endswith(SIMPLE_FITS_SUFFIXES):
        return name
    if lower_name.endswith('.fits.fz') or lower_name.endswith('.fits.gz'):
        return name[:-3]
    if lower_name.endswith('.fz') or lower_name.endswith('.gz'):
        return f'{name[:-3]}.fits'
    return f'{name}.fits'


def _get_first_image_hdu(hdulist):
    for hdu in hdulist:
        if getattr(hdu, 'data', None) is not None:
            return hdu
    raise ValueError('FITS file does not contain an image HDU.')


def _build_primary_header_from_hdu_headers(*headers):
    skip_exact = {
        'SIMPLE',
        'BITPIX',
        'NAXIS',
        'EXTEND',
        'EXTNAME',
        'EXTVER',
        'XTENSION',
        'PCOUNT',
        'GCOUNT',
        'CHECKSUM',
        'DATASUM',
    }
    skip_prefixes = (
        'NAXIS',
        'ZNAXIS',
        'ZTILE',
        'ZNAME',
        'ZVAL',
        'ZDITHER',
    )
    skip_exact.update({
        'ZIMAGE',
        'ZCMPTYPE',
        'ZBITPIX',
        'ZBLANK',
        'ZDATASUM',
        'ZHECKSUM',
    })

    clean_header = fits.Header()
    for header in headers:
        if header is None:
            continue
        for card in header.cards:
            keyword = str(card.keyword or '').strip()
            if keyword == '' or keyword == 'COMMENT' or keyword == 'HISTORY':
                clean_header.append(card)
                continue
            if keyword in skip_exact:
                continue
            if any(keyword.startswith(prefix) for prefix in skip_prefixes):
                continue
            clean_header[keyword] = (card.value, card.comment)
    return clean_header


def _build_simple_fits_content(file_content):
    with fits.open(io.BytesIO(file_content)) as hdulist:
        primary_header = hdulist[0].header if hdulist else None
        image_hdu = _get_first_image_hdu(hdulist)
        output = io.BytesIO()
        header = _build_primary_header_from_hdu_headers(primary_header, image_hdu.header)
        fits.PrimaryHDU(data=image_hdu.data, header=header).writeto(output, overwrite=True)
        return output.getvalue()


def _sanitize_extension_header(header, *, keep_extname=True):
    skip_exact = {
        'SIMPLE',
        'ZIMAGE',
        'ZCMPTYPE',
        'ZBITPIX',
        'ZBLANK',
        'ZDATASUM',
        'ZHECKSUM',
        'CHECKSUM',
        'DATASUM',
    }
    skip_prefixes = (
        'ZNAXIS',
        'ZTILE',
        'ZNAME',
        'ZVAL',
        'ZDITHER',
    )
    clean_header = fits.Header()
    for card in header.cards:
        keyword = str(card.keyword or '').strip()
        if keyword == '' or keyword == 'COMMENT' or keyword == 'HISTORY':
            clean_header.append(card)
            continue
        if keyword in skip_exact:
            continue
        if keyword == 'EXTNAME' and not keep_extname:
            continue
        if any(keyword.startswith(prefix) for prefix in skip_prefixes):
            continue
        clean_header[keyword] = (card.value, card.comment)
    return clean_header


def _build_funpack_like_fits_content(file_content):
    with fits.open(io.BytesIO(file_content)) as hdulist:
        output_hdus = []
        for hdu in hdulist:
            data = getattr(hdu, 'data', None)
            name = str(getattr(hdu, 'name', '') or '').strip().upper()
            if data is None:
                continue
            if not output_hdus:
                primary_header = _sanitize_extension_header(hdu.header, keep_extname=True)
                output_hdus.append(fits.PrimaryHDU(data=data, header=primary_header))
                continue
            if isinstance(hdu, (fits.BinTableHDU, fits.TableHDU)):
                table_header = _sanitize_extension_header(hdu.header, keep_extname=True)
                output_hdus.append(fits.BinTableHDU(data=data, header=table_header, name=name or None))
            else:
                image_header = _sanitize_extension_header(hdu.header, keep_extname=True)
                output_hdus.append(fits.ImageHDU(data=data, header=image_header, name=name or None))

        if not output_hdus:
            raise ValueError('FITS file does not contain any HDUs with data.')

        output = io.BytesIO()
        fits.HDUList(output_hdus).writeto(output, overwrite=True, checksum=True)
        return output.getvalue()


def _decompress_fpack_content(file_content, file_name):
    funpack_path = shutil.which('funpack')
    if not funpack_path:
        return None

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_input_name = os.path.join(temp_dir, 'input.fits.fz')
        temp_output_name = os.path.join(temp_dir, 'output.fits')

        with open(temp_input_name, 'wb') as temp_input:
            temp_input.write(file_content)

        result = subprocess.run(
            [funpack_path, '-O', temp_output_name, temp_input_name],
            capture_output=True,
            check=True,
        )
        if not os.path.exists(temp_output_name):
            raise OSError(f'funpack did not create output file for {file_name}')

        with open(temp_output_name, 'rb') as decompressed_handle:
            decompressed_content = decompressed_handle.read()

        logger.info(
            'Decompressed FPACK file with funpack: %s stdout=%d stderr=%d output_bytes=%d',
            file_name,
            len(result.stdout or b''),
            len(result.stderr or b''),
            len(decompressed_content),
        )
        return decompressed_content


def normalize_fits_upload(uploaded_file):
    lower_name = str(uploaded_file.name or '').lower()
    metadata = {
        'recognized_format': _detect_fits_upload_format(uploaded_file.name),
        'decompression_method': 'none',
    }
    if not lower_name.endswith(COMPRESSED_FITS_SUFFIXES):
        uploaded_file.seek(0)
        uploaded_file.name = _coerce_simple_fits_name(uploaded_file.name)
        return uploaded_file, metadata

    uploaded_file.seek(0)
    file_content = uploaded_file.read()
    uploaded_file.seek(0)

    if lower_name.endswith('.gz'):
        file_content = gzip.decompress(file_content)
        metadata['decompression_method'] = 'gzip'
    elif lower_name.endswith('.fz'):
        try:
            decompressed_content = _decompress_fpack_content(file_content, uploaded_file.name)
            if decompressed_content is not None:
                file_content = decompressed_content
                metadata['decompression_method'] = 'funpack'
            else:
                logger.warning('funpack not available, falling back to astropy for %s', uploaded_file.name)
                metadata['decompression_method'] = 'astropy'
        except (subprocess.CalledProcessError, OSError) as exc:
            logger.warning('funpack failed for %s, falling back to astropy', uploaded_file.name)
            logger.exception(exc)
            metadata['decompression_method'] = 'astropy'

    normalized_name = _coerce_simple_fits_name(uploaded_file.name)
    if lower_name.endswith('.fz'):
        if metadata['decompression_method'] == 'funpack':
            normalized_content = file_content
        else:
            normalized_content = _build_funpack_like_fits_content(file_content)
    else:
        normalized_content = _build_simple_fits_content(file_content)
    return SimpleUploadedFile(
        normalized_name,
        normalized_content,
        content_type='application/fits',
    ), metadata


def build_bhtom2_upload_payload(dataproduct, observatory, calibration_filter, comment=''):
    return {
        'target': dataproduct.target.name,
        'data_product_type': 'fits_file',
        'priority': 2,
        'observatory': observatory,
        'filter': calibration_filter,
        'comment': comment,
        'no_plot': False,
    }


def _upload_service_url():
    return str(getattr(settings, 'BHTOM2_UPLOAD_SERVICE_URL', '') or '').strip()


def _upload_timeout():
    try:
        return max(1, int(getattr(settings, 'BHTOM2_UPLOAD_TIMEOUT', getattr(settings, 'BHTOM2_API_TIMEOUT', 30))))
    except (TypeError, ValueError):
        return max(1, int(getattr(settings, 'BHTOM2_API_TIMEOUT', 30)))


def load_extra_data_dict(dataproduct):
    raw_value = getattr(dataproduct, 'extra_data', '') or ''
    if isinstance(raw_value, dict):
        return dict(raw_value)
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value)
    except (TypeError, ValueError):
        return {'legacy_text': str(raw_value)}
    return parsed if isinstance(parsed, dict) else {}


def save_extra_data_dict(dataproduct, payload):
    dataproduct.extra_data = json.dumps(payload, sort_keys=True)
    dataproduct.save(update_fields=['extra_data'])


def get_bhtom2_upload_state(dataproduct):
    return load_extra_data_dict(dataproduct).get(BHTOM2_UPLOAD_STATE_KEY) or {}


def has_successful_bhtom2_upload(dataproduct):
    return get_bhtom2_upload_state(dataproduct).get('status') == 'uploaded'


def record_bhtom2_upload_state(dataproduct, *, status, observatory='', calibration_filter='', response_status=None,
                               message='', upload_metadata=None, user_id=None):
    payload = load_extra_data_dict(dataproduct)
    payload[BHTOM2_UPLOAD_STATE_KEY] = {
        'status': status,
        'observatory': observatory,
        'calibration_filter': calibration_filter,
        'response_status': response_status,
        'message': message,
        'upload_metadata': upload_metadata or {},
        'user_id': user_id,
        'updated_at': datetime.now(timezone.utc).isoformat(),
    }
    save_extra_data_dict(dataproduct, payload)


def ensure_fits_dataproduct_type(dataproduct):
    if getattr(dataproduct, 'data_product_type', '') == 'fits_file':
        return True
    filename = ''
    try:
        filename = dataproduct.get_file_name()
    except Exception:
        filename = getattr(getattr(dataproduct, 'data', None), 'name', '')
    if not is_supported_fits_filename(filename):
        return False
    dataproduct.data_product_type = 'fits_file'
    dataproduct.save(update_fields=['data_product_type'])
    return True


def upload_fits_dataproduct_to_bhtom2(dataproduct, *, token, observatory, calibration_filter, comment=''):
    upload_url = _upload_service_url()
    if not upload_url:
        raise Bhtom2UploadError('BHTOM2 upload service URL is not configured.')
    if not str(token or '').strip():
        raise Bhtom2UploadError('BHTOM2 token is required.')
    if not str(observatory or '').strip():
        raise Bhtom2UploadError('BHTOM2 ONAME is required.')

    with dataproduct.data.open('rb') as data_handle:
        upload_file = SimpleUploadedFile(
            dataproduct.get_file_name(),
            data_handle.read(),
            content_type='application/octet-stream',
        )
    logger.info(
        'Preparing BHTOM2 upload for dataproduct=%s original_name=%s observatory=%s',
        getattr(dataproduct, 'pk', None),
        upload_file.name,
        observatory,
    )
    normalized_file, upload_metadata = normalize_fits_upload(upload_file)
    normalized_file.seek(0)
    normalized_file.name = _coerce_simple_fits_name(normalized_file.name)
    logger.info(
        'Normalized BHTOM2 upload for dataproduct=%s normalized_name=%s metadata=%s',
        getattr(dataproduct, 'pk', None),
        normalized_file.name,
        upload_metadata,
    )

    response = requests.post(
        upload_url,
        data=build_bhtom2_upload_payload(dataproduct, observatory, calibration_filter, comment=comment),
        files={'file_0': (normalized_file.name, normalized_file, normalized_file.content_type or 'application/fits')},
        headers={'Authorization': f'Token {str(token).strip()}'},
        timeout=_upload_timeout(),
    )
    logger.info(
        'BHTOM2 upload response for dataproduct=%s status=%s body=%s',
        getattr(dataproduct, 'pk', None),
        response.status_code,
        response.text[:500],
    )
    return response, upload_metadata


def forward_dataproduct_to_bhtom2(dataproduct, *, token, observatory, calibration_filter, comment='',
                                  user_id=None):
    try:
        response, upload_metadata = upload_fits_dataproduct_to_bhtom2(
            dataproduct,
            token=token,
            observatory=observatory,
            calibration_filter=calibration_filter,
            comment=comment,
        )
    except requests.RequestException as exc:
        record_bhtom2_upload_state(
            dataproduct,
            status='failed',
            observatory=observatory,
            calibration_filter=calibration_filter,
            message=str(exc),
            user_id=user_id,
        )
        raise
    except Exception as exc:
        record_bhtom2_upload_state(
            dataproduct,
            status='failed',
            observatory=observatory,
            calibration_filter=calibration_filter,
            message=str(exc),
            user_id=user_id,
        )
        raise

    if response.status_code != 201:
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        message = payload.get('message') or payload.get('detail') or response.text.strip() or f'HTTP {response.status_code}'
        record_bhtom2_upload_state(
            dataproduct,
            status='failed',
            observatory=observatory,
            calibration_filter=calibration_filter,
            response_status=response.status_code,
            message=message,
            upload_metadata=upload_metadata,
            user_id=user_id,
        )
        raise ValidationError(message)

    record_bhtom2_upload_state(
        dataproduct,
        status='uploaded',
        observatory=observatory,
        calibration_filter=calibration_filter,
        response_status=response.status_code,
        message='Uploaded to BHTOM2.',
        upload_metadata=upload_metadata,
        user_id=user_id,
    )
    return response
