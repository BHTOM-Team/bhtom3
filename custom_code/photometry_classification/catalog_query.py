import logging

import astropy.units as u
from astropy.coordinates import SkyCoord
from astroquery.gaia import Gaia
from astroquery.vizier import Vizier
from typing import Any, Callable, Dict, List
from functools import lru_cache

import numpy as np
import pandas as pd

logging.getLogger("astroquery").setLevel(logging.WARNING)

GAIA_photometry_columns: List[str] = ['phot_g_mean_mag',
                                      'phot_rp_mean_mag',
                                      'phot_bp_mean_mag']

TWOMASS_photometry_columns: List[str] = ['Hmag', 'Jmag', 'Kmag']

ALLWISE_photometry_columns: List[str] = ['W1mag', 'W2mag']

GAIA_photometry_columns_mapping: Dict[str, str] = {
    'phot_g_mean_mag': 'g',
    'phot_rp_mean_mag': 'rp',
    'phot_bp_mean_mag': 'bp',
}

TWOMASS_photometry_columns_mapping: Dict[str, str] = {
    'Hmag': 'h',
    'Jmag': 'j',
    'Kmag': 'k',
}

ALLWISE_photometry_columns_mapping: Dict[str, str] = {
    'W1mag': 'w1',
    'W2mag': 'w2'
}

VIZIER_2MASS_name: str = "II/246"

VIZIER_ALLWISE_name: str = "II/328/allwise"


@lru_cache()
def query_for_object(ra: float,
                     dec: float) -> pd.DataFrame:
    gaia_photometry: pd.DataFrame = query_service(ra, dec,
                                                  query_method=query_gaia,
                                                  columns=GAIA_photometry_columns,
                                                  columns_mapping=GAIA_photometry_columns_mapping)
    twomass_photometry: pd.DataFrame = query_service(ra, dec,
                                                     query_method=query_twomass,
                                                     columns=TWOMASS_photometry_columns,
                                                     columns_mapping=TWOMASS_photometry_columns_mapping)

    wise_photometry: pd.DataFrame = query_service(ra, dec,
                                                  query_method=query_wise,
                                                  columns=ALLWISE_photometry_columns,
                                                  columns_mapping=ALLWISE_photometry_columns_mapping)

    return pd.concat([gaia_photometry, twomass_photometry, wise_photometry],
                     axis=1).dropna()


def filter_results(result_table, columns) -> np.array:
    return np.array([list(row) for row in result_table[columns] if not np.isnan(list(row)).any()])


def query_service(ra: float,
                  dec: float,
                  query_method: Callable[[SkyCoord, u.Quantity], Any],
                  columns: List[str],
                  columns_mapping: Dict[str, str]) -> pd.DataFrame:
    coord: SkyCoord = SkyCoord(ra=ra,
                               dec=dec,
                               unit=(u.degree, u.degree),
                               frame='icrs')
    radius = u.Quantity(0.001, u.deg)

    try:
        r = query_method(coord, radius)
    except Exception as e:
        return pd.DataFrame()

    if len(r) > 0:
        photometry: np.array = filter_results(r, columns)

        if len(photometry) > 0:
            try:
                photometry_df: pd.DataFrame = pd.DataFrame(data=photometry,
                                                           columns=[columns_mapping[c] for c in columns])

                return photometry_df

            except:
                return pd.DataFrame()

    return pd.DataFrame()


def query_gaia(coord: SkyCoord,
               radius: u.Quantity) -> Any:
    Gaia.MAIN_GAIA_TABLE = "gaiaedr3.gaia_source"
    return Gaia.cone_search_async(coordinate=coord, radius=radius).get_results()


def query_twomass(coord: SkyCoord,
                  radius: u.Quantity) -> Any:
    table = Vizier.query_region(coord,
                                radius=radius,
                                catalog=VIZIER_2MASS_name)

    return table[0] if len(table) > 0 else []


def query_wise(coord: SkyCoord,
               radius: u.Quantity) -> Any:
    table = Vizier.query_region(coord,
                                radius=radius,
                                catalog=VIZIER_ALLWISE_name)

    return table[0] if len(table) > 0 else []
