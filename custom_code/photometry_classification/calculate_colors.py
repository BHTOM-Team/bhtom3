import re

import pandas as pd
from typing import List
from .classification_source import ClassificationSource, SELECTED_FEATURES


GAIA_COLORS: List[str] = ['g', 'rp', 'bp']
TWOMASS_COLORS: List[str] = ['h', 'j', 'k']
ALLWISE_COLORS: List[str] = ['w1', 'w2']

COLOR_REGEX = r'([A-Z|a-z|\d]{1,2})mag-([A-Z|a-z|\d]{1,2})mag'


def colors_df(magnitudes: pd.DataFrame) -> pd.DataFrame:
    colors: pd.DataFrame = pd.DataFrame()

    def calculate_color(column_name: str):
        try:
            match = re.search(COLOR_REGEX, column_name)
            first_color = match.group(1)
            second_color = match.group(2)
            colors[column_name] = magnitudes[first_color.lower()]-magnitudes[second_color.lower()]
        except:
            pass

    # All Gaia, 2MASS and WISE are available:
    if set(GAIA_COLORS+TWOMASS_COLORS+ALLWISE_COLORS).issubset(set(magnitudes.columns)):
        for feature in SELECTED_FEATURES[ClassificationSource.WISE]:
            calculate_color(feature)

    # Only Gaia and 2MASS are available:
    elif set(GAIA_COLORS+TWOMASS_COLORS).issubset(set(magnitudes.columns)):
        for feature in SELECTED_FEATURES[ClassificationSource.TWO_MASS]:
            calculate_color(feature)

    # Only Gaia
    elif set(GAIA_COLORS).issubset(set(magnitudes.columns)):
        for feature in SELECTED_FEATURES[ClassificationSource.GAIA]:
            calculate_color(feature)

    return colors
