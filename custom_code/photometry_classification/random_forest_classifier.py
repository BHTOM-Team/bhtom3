import logging
import pickle
from typing import List

import numpy as np
import pandas as pd
from sklearn import metrics
from sklearn.ensemble import RandomForestClassifier

from sklearn.model_selection import KFold

from .classification_source import ClassificationSource, SELECTED_FEATURES, TRAINING_FILE_PATHS, MODEL_PATHS


logger: logging.Logger = logging.getLogger(__name__)


def empty_training_file(source: ClassificationSource) -> pd.DataFrame:
    return pd.DataFrame(columns=[[*SELECTED_FEATURES.get(source, []), 'class']])


def read_training_data(source: ClassificationSource) -> pd.DataFrame:
    filename: str = TRAINING_FILE_PATHS.get(source, '')
    try:
        gaia_df: pd.DataFrame = pd.read_csv(filename)[[*SELECTED_FEATURES.get(source, []), 'class']]
        return gaia_df
    except FileNotFoundError:
        logger.error(f'File {filename} not found!')
        return empty_training_file(source)
    except pd.errors.ParserError as e:
        logger.error(f'Parser error while reading {filename}: {e}')
        return empty_training_file(source)
    except Exception as e:
        logger.error(f'Unexpected error while reading {filename}: {e}')
        return empty_training_file(source)


def train_rfc(source: ClassificationSource) -> RandomForestClassifier:
    np.random.seed(42)
    rfc: RandomForestClassifier = RandomForestClassifier(n_estimators=500, random_state=42)
    kf = KFold(n_splits=9, random_state=42, shuffle=True)
    training_data: pd.DataFrame = read_training_data(source)
    selected_features: List[str] = SELECTED_FEATURES.get(source, [])
    kf.get_n_splits(training_data)
    for train_index, test_index in kf.split(training_data):
        train, test = training_data.loc[train_index], training_data.loc[test_index]
        train_features, train_class = train.loc[:, [*selected_features]], train.loc[:, 'class']

        test_features, test_class = test.loc[:, [*selected_features]], test.loc[:, 'class']

        rfc.fit(train_features, train_class)

        metrics.accuracy_score(test_class, rfc.predict(test_features))
    return rfc


def load_rfc(source: ClassificationSource) -> RandomForestClassifier:
    try:
        with open(MODEL_PATHS.get(source, ''), 'rb') as handle:
            rfc: RandomForestClassifier = pickle.load(handle)
            return rfc
    except Exception as e:
        logger.warning('Could not load RFC for %s from file; retraining model. Reason: %s', source.name, e)
        rfc: RandomForestClassifier = train_rfc(source)
        with open(MODEL_PATHS.get(source, ''), 'wb') as handle:
            pickle.dump(rfc, handle)
        return rfc
