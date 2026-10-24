from io import StringIO

import pandas as pd
import requests

from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT
from custom_code.alias_astrometry import matched_position


def build_allwise_source_query(ra, dec, radius_arcsec):
    return (
        'https://irsa.ipac.caltech.edu/cgi-bin/Gator/nph-query?catalog=allwise_p3as_psd'
        f'&spatial=cone&radius={radius_arcsec}&radunits=arcsec&objstr={ra}+{dec}'
        '&outfmt=1&selcols=ra,dec,designation,sigra,sigdec,sigradec'
    )


def format_allwise_alias(designation):
    designation = str(designation or '').strip()
    if not designation:
        return None
    return f'WISEA J{designation}'


def fetch_allwise_alias(ra, dec, radius_arcsec):
    match = fetch_allwise_match(ra, dec, radius_arcsec)
    return match['name'] if match else None


def fetch_allwise_match(ra, dec, radius_arcsec):
    response = requests.get(
        build_allwise_source_query(ra, dec, radius_arcsec),
        timeout=DATA_SERVICE_HTTP_TIMEOUT,
    )
    if not response.text.strip():
        return None

    response_table = response.text.split('null|\n', 1)[1]
    data = pd.read_csv(
        StringIO(response_table),
        header=None,
        names=['ra', 'dec', 'clon', 'clat', 'designation', 'sigra', 'sigdec', 'sigradec', 'dist', 'angle'],
        sep=r'\s+',
    )
    if len(data) < 1:
        return None

    row = data.iloc[0]
    position = matched_position(row['ra'], row['dec'], row.get('sigra'), row.get('sigdec'))
    # IRSA quotes a signed co-sigma in arcseconds, rather than covariance.
    # https://irsa.ipac.caltech.edu/data/WISE/docs/release/AllWISE/expsup/sec2_1a.html
    if position.get('ra_error_arcsec', 0) > 0 and position.get('dec_error_arcsec', 0) > 0:
        cosigma = row.get('sigradec')
        if pd.notna(cosigma):
            correlation = float(cosigma) * abs(float(cosigma)) / (position['ra_error_arcsec'] * position['dec_error_arcsec'])
            if -1 <= correlation <= 1:
                position['ra_dec_corr'] = correlation
    return {'name': format_allwise_alias(row['designation']), 'matched_position': position}
