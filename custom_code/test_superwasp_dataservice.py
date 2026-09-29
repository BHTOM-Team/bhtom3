from datetime import datetime, timezone
from unittest.mock import Mock, patch

from astropy.time import Time
from django.test import SimpleTestCase

from custom_code.data_services.superwasp_dataservice import (
    CORRECTED_FILTER,
    RAW_FILTER,
    SuperWASPAmbiguousMatchError,
    SuperWASPDataService,
    hjd_utc_to_bjd_tdb,
    parse_superwasp_ipac,
)
from custom_code.tasks import _bulk_insert_reduced_datums
from custom_code.data_services.service_utils import upsert_reduced_datums
from custom_code.templatetags.custom_dataproduct_extras import _photometry_trace_visibility


SOURCE_ID = '1SWASP J150658.93-313838.9'
SOURCE_URL = (
    'https://exoplanetarchive.ipac.caltech.edu/data/ETSS/SuperWASP/TBL/DR1/'
    'tile222054/1SWASP_J150658.93-313838.9_lc.tbl'
)

SAMPLE_IPAC = r"""\OBJNAME = '1SWASP J150658.93-313838.9'
\OBSSTART = '2006-05-04T21:13:18'
\OBSSTOP = '2006-05-04T21:13:47'
\TSTART = 73862495
\TSTOP = 73862524
\JD_REF = 2453005.5
\RA_OBJ = '226.745544'
\DEC_OBJ = '-31.644161'
\NUMRECORDS = 2
|TMID      |FLUX2         |FLUX2_ERR     |TAMFLUX2      |TAMFLUX2_ERR  |IMAGEID           |CCDX      |CCDY      |FLAG |           HJD|         MAG2|     MAG2_ERR|      TAMMAG2|  TAMMAG2_ERR|
|int       |float         |float         |float         |float         |char              |int       |int       |int  |        double|        float|        float|        float|        float|
|sec       |micro Vega    |micro Vega    |micro Vega    |micro Vega    |                  |1/16 pixel|1/16 pixel|     |           day|          mag|          mag|          mag|          mag|
|null      |null          |null          |null          |null          |null              |null      |null      |null |          null|         null|         null|         null|         null|
   73862495   2.277175e+02   3.615729e-01   2.389207e+02   1.546886e+00 221200605042113180        745      20463    32 2453860.389988  9.106509e+00  1.723952e-03  9.054365e+00  7.029595e-03
   73862524   2.291152e+02   3.622131e-01   2.405482e+02   1.324412e+00 221200605042113470        744      20463     0 2453860.390324  9.099866e+00  1.716470e-03  9.046995e+00  5.977873e-03
"""


def sample_metadata():
    return {
        'sourceid': SOURCE_ID,
        'ra': 226.745544,
        'dec': -31.644161,
        'tile': 'tile222054',
        'npts': 2,
        'hjd_ref': 2453005.5,
        'separation_arcsec': 0.071,
    }


class SuperWASPParsingTests(SimpleTestCase):
    def test_service_exposes_download_acknowledgement(self):
        acknowledgement = SuperWASPDataService.get_acknowledgement()

        self.assertIn('first public release of the WASP data', acknowledgement)
        self.assertIn('Butters et al. 2010', acknowledgement)
        self.assertIn('NASA Exoplanet Archive', acknowledgement)
        self.assertIn('10.26133/NEA9', acknowledgement)
        self.assertEqual(SuperWASPDataService.acknowledgement_doi, '10.26133/NEA9')
        self.assertEqual(SuperWASPDataService.acknowledgement_url, SuperWASPDataService.info_url)

    def test_target_plot_shows_corrected_series_and_keeps_raw_in_legend(self):
        self.assertIs(_photometry_trace_visibility(CORRECTED_FILTER), True)
        self.assertEqual(_photometry_trace_visibility(RAW_FILTER), 'legendonly')

    def test_parses_corrected_and_raw_series_with_archive_provenance(self):
        datums, header = parse_superwasp_ipac(
            SAMPLE_IPAC,
            metadata=sample_metadata(),
            source_url=SOURCE_URL,
            retrieved_at=datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(header['NUMRECORDS'], 2)
        self.assertEqual(len(datums), 4)
        corrected, raw = datums[:2]
        self.assertEqual(corrected['value']['filter'], CORRECTED_FILTER)
        self.assertEqual(corrected['value']['magnitude'], 9.054365)
        self.assertEqual(corrected['value']['error'], 0.007029595)
        self.assertEqual(raw['value']['filter'], RAW_FILTER)
        self.assertEqual(raw['value']['magnitude'], 9.106509)
        self.assertEqual(corrected['value']['original_hjd_utc'], 2453860.389988)
        self.assertEqual(corrected['value']['time_standard'], 'HJD_UTC')
        self.assertEqual(corrected['value']['image_id'], '221200605042113180')
        self.assertEqual(corrected['value']['camera_id'], '221')
        self.assertEqual(corrected['value']['quality_flag'], 32)
        self.assertEqual(corrected['value']['wasp_ra_deg'], 226.745544)
        self.assertEqual(corrected['value']['wasp_dec_deg'], -31.644161)
        self.assertEqual(corrected['value']['data_release'], 'WASP DR1')
        self.assertEqual(corrected['value']['doi'], '10.26133/NEA9')
        self.assertEqual(corrected['value']['source_url'], SOURCE_URL)

    def test_hjd_utc_mapping_and_explicit_bjd_tdb_conversion(self):
        datums, _header = parse_superwasp_ipac(
            SAMPLE_IPAC,
            metadata=sample_metadata(),
            source_url=SOURCE_URL,
            retrieved_at=datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc),
            include_raw=False,
        )
        mapped_jd = Time(datums[0]['timestamp'], scale='utc').jd
        self.assertAlmostEqual(mapped_jd, 2453860.389988, places=8)
        self.assertIn('not BJD_TDB', datums[0]['value']['timestamp_mapping'])

        bjd_tdb = hjd_utc_to_bjd_tdb(2453860.389988, 226.745544, -31.644161)
        self.assertAlmostEqual(bjd_tdb, 2453860.390717666, places=7)
        self.assertNotAlmostEqual(bjd_tdb, 2453860.389988, places=5)

    def test_ambiguous_coordinate_match_is_rejected_with_candidates(self):
        service = SuperWASPDataService()
        matches = [
            {**sample_metadata(), 'sourceid': SOURCE_ID, 'separation_arcsec': 0.1},
            {
                **sample_metadata(),
                'sourceid': '1SWASP J150658.90-313839.0',
                'separation_arcsec': 0.8,
            },
        ]
        with patch.object(service, '_fetch_metadata', return_value=matches):
            with self.assertRaisesMessage(SuperWASPAmbiguousMatchError, 'Supply an exact') as raised:
                service.query_service({
                    'ra': 226.745544,
                    'dec': -31.644161,
                    'radius_arcsec': 5.0,
                    'include_photometry': True,
                })
        self.assertIn(SOURCE_ID, str(raised.exception))
        self.assertIn('1SWASP J150658.90-313839.0', str(raised.exception))


class SuperWASPImportTests(SimpleTestCase):
    def test_repeated_import_is_idempotent_by_observation_key(self):
        first, _ = parse_superwasp_ipac(
            SAMPLE_IPAC,
            metadata=sample_metadata(),
            source_url=SOURCE_URL,
            retrieved_at=datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc),
        )
        refreshed, _ = parse_superwasp_ipac(
            SAMPLE_IPAC,
            metadata=sample_metadata(),
            source_url=SOURCE_URL,
            retrieved_at=datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc),
        )
        rows = []

        class FakeReducedDatum:
            objects = None

            def __init__(self, **kwargs):
                self.pk = None
                for key, value in kwargs.items():
                    setattr(self, key, value)

        class FakeManager:
            def filter(self, **kwargs):
                timestamps = kwargs.get('timestamp__in', set())
                return [
                    row for row in rows
                    if row.target is kwargs['target']
                    and row.data_type == kwargs['data_type']
                    and row.source_name == kwargs['source_name']
                    and row.timestamp in timestamps
                ]

            def bulk_create(self, values, batch_size=None):
                for value in values:
                    value.pk = len(rows) + 1
                    rows.append(value)

            def bulk_update(self, values, fields, batch_size=None):
                return None

        FakeReducedDatum.objects = FakeManager()
        target = object()
        with patch('tom_dataproducts.models.ReducedDatum', FakeReducedDatum):
            first_added, _ = upsert_reduced_datums(
                target, 'photometry', 'SuperWASP', SOURCE_URL, first,
                identity_keys=('observation_key', 'filter'),
            )
            second_added, _ = upsert_reduced_datums(
                target, 'photometry', 'SuperWASP', SOURCE_URL, refreshed,
                identity_keys=('observation_key', 'filter'),
            )

        self.assertEqual(first_added, 4)
        self.assertEqual(second_added, 0)
        self.assertEqual(len(rows), 4)
        self.assertEqual(sum(row.value['filter'] == CORRECTED_FILTER for row in rows), 2)
        # Provenance from the first retrieval is retained rather than producing duplicates.
        self.assertTrue(all(row.value['retrieved_at'].startswith('2026-09-28') for row in rows))

    def test_scheduled_bulk_path_uses_service_identity_keys(self):
        service = SuperWASPDataService()
        datum = {
            'timestamp': datetime(2006, 5, 4, tzinfo=timezone.utc),
            'value': {
                'filter': CORRECTED_FILTER,
                'magnitude': 9.05,
                'observation_key': 'WASP DR1:test:1:image',
            },
        }
        with patch('custom_code.tasks.upsert_reduced_datums', return_value=(1, 0)) as upsert:
            added = _bulk_insert_reduced_datums(
                Mock(), service.name, service, {'source_location': SOURCE_URL},
                {'photometry': [datum]},
            )

        self.assertEqual(added, 1)
        self.assertEqual(upsert.call_args.kwargs['identity_keys'], ('observation_key', 'filter'))
