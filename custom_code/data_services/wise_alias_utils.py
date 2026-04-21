from io import StringIO

import pandas as pd
import requests


def build_allwise_source_query(ra, dec, radius_arcsec):
    return (
        'https://irsa.ipac.caltech.edu/cgi-bin/Gator/nph-query?catalog=allwise_p3as_psd'
        f'&spatial=cone&radius={radius_arcsec}&radunits=arcsec&objstr={ra}+{dec}'
        '&outfmt=1&selcols=ra,dec,designation'
    )


def format_allwise_alias(designation):
    designation = str(designation or '').strip()
    if not designation:
        return None
    return f'WISEA J{designation}'


def fetch_allwise_alias(ra, dec, radius_arcsec):
    response = requests.get(build_allwise_source_query(ra, dec, radius_arcsec))
    if not response.text.strip():
        return None

    response_table = response.text.split('null|\n', 1)[1]
    data = pd.read_csv(
        StringIO(response_table),
        header=None,
        names=['ra', 'dec', 'clon', 'clat', 'designation', 'dist', 'angle'],
        sep=r'\s+',
    )
    if len(data) < 1:
        return None

    return format_allwise_alias(data.iloc[0]['designation'])
