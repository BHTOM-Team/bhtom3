import os
import tempfile

import numpy as np
import requests
from django.core.management.base import BaseCommand, CommandError

from custom_code.data_services.icecube_dr2_dataservice import (
    DATAVERSE_DOI,
    ICECUBE_DR2_DATA_PATH,
    MJD_QUANTUM,
    SCALES,
)

DATAVERSE_API = 'https://dataverse.harvard.edu/api'
# Harvard Dataverse answers 403 to the default python-requests/urllib user agents.
HEADERS = {'User-Agent': 'bhtom3 IceCube DR2 builder'}


def _parse_events(path):
    """One season's events file -> (N, 8) float array. Dataverse serves each row quoted."""
    rows = []
    with open(path) as handle:
        for line in handle:
            if line.startswith('#'):
                continue
            fields = line.replace('"', '').split()
            if len(fields) == 10:
                # run, event, subevent, MJD, log10(E/GeV), AngErr, RA, Dec (azimuth/zenith dropped)
                rows.append(fields[:8])
    return np.array(rows, dtype=float).reshape(-1, 8)


def _quantize(values, scale, dtype):
    scaled = np.rint(values * scale)
    if np.max(np.abs(scaled - values * scale)) > 0.01:
        raise CommandError(f'Values are finer than 1/{scale}; the encoding would lose precision.')
    info = np.iinfo(dtype)
    if scaled.min() < info.min or scaled.max() > info.max:
        raise CommandError(f'Values overflow {np.dtype(dtype).name} at scale {scale}.')
    return scaled.astype(dtype)


class Command(BaseCommand):
    help = (
        "Download the IceCube IceTracks-DR2 event files from Harvard Dataverse and pack them into the "
        "compressed table the IceCube DR2 data service reads. Encoding is lossless at the published "
        "precision; azimuth and zenith are dropped."
    )

    def add_arguments(self, parser):
        parser.add_argument('--source-dir', help='Use already-downloaded *_exp.tab event files from this directory.')
        parser.add_argument('--output', default=ICECUBE_DR2_DATA_PATH, help='Output .npz path.')

    def handle(self, *args, **options):
        response = requests.get(
            f'{DATAVERSE_API}/datasets/:persistentId/', params={'persistentId': DATAVERSE_DOI}, headers=HEADERS, timeout=60,
        )
        response.raise_for_status()
        dataset = response.json()['data']['latestVersion']
        version = f"{dataset['versionNumber']}.{dataset['versionMinorNumber']}"
        files = sorted(
            (f['label'], f['dataFile']['id'])
            for f in dataset['files'] if f.get('directoryLabel') == 'events'
        )
        if not files:
            raise CommandError('No files in the dataset\'s events/ folder.')

        with tempfile.TemporaryDirectory() as tmp:
            source_dir = options.get('source_dir') or tmp
            seasons, chunks = [], []
            for season_index, (label, file_id) in enumerate(files):
                path = os.path.join(source_dir, label)
                if not os.path.exists(path):
                    self.stdout.write(f'Downloading {label} ...')
                    response = requests.get(f'{DATAVERSE_API}/access/datafile/{file_id}', headers=HEADERS, timeout=600)
                    response.raise_for_status()
                    with open(path, 'wb') as out:
                        out.write(response.content)
                events = _parse_events(path)
                self.stdout.write(f'{label}: {len(events)} events')
                seasons.append(label.replace('_exp.tab', ''))
                chunks.append(np.column_stack([events, np.full(len(events), season_index)]))

        data = np.concatenate(chunks)
        data = data[np.lexsort((data[:, 2], data[:, 1], data[:, 3]))]  # by time, then event, subevent

        mjd_ticks = _quantize(data[:, 3], 1 / MJD_QUANTUM, np.int64)
        np.savez_compressed(
            options['output'],
            # Time as differences: sorted arrival times compress far better that way.
            mjd_start=mjd_ticks[:1],
            mjd_delta=np.diff(mjd_ticks).astype(np.uint32),
            run=data[:, 0].astype(np.uint32),
            event=data[:, 1].astype(np.uint32),
            subevent=data[:, 2].astype(np.uint8),
            log10_energy=_quantize(data[:, 4], SCALES['log10_energy'], np.uint16),
            ang_err=_quantize(data[:, 5], SCALES['ang_err'], np.uint16),
            ra=_quantize(data[:, 6], SCALES['ra'], np.uint32),
            dec=_quantize(data[:, 7], SCALES['dec'], np.int32),
            season=data[:, 8].astype(np.uint8),
            seasons=np.array(seasons),
            dataverse_version=np.array(version),
        )
        size_mb = os.path.getsize(options['output']) / 1e6
        self.stdout.write(self.style.SUCCESS(
            f'Wrote {len(data)} events from {len(files)} seasons (Dataverse v{version}) '
            f'to {options["output"]} ({size_mb:.1f} MB).'
        ))
