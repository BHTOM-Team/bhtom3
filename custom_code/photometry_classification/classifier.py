import numpy as np
import logging
from typing import Dict, List, Tuple

from sklearn.ensemble import RandomForestClassifier
from .random_forest_classifier import load_rfc
from .classification_source import ClassificationSource

logger: logging.Logger = logging.getLogger(__name__)


CLASSIFIER_LABELS: Dict[ClassificationSource, Dict[str, str]] = {
    ClassificationSource.GAIA: {
        'C': 'Ulens Candidate',
        'N': 'Not Ulens'
    },
    ClassificationSource.TWO_MASS: {
        'B': 'Be Star',
        'U': 'Ulens Candidate',
        'E': 'Evolved',
        'Y': 'YSO'
    },
    ClassificationSource.WISE: {
        'B': 'Be Star',
        'S': 'Main Sequence Star',
        'R': 'Red Giant',
        'E': 'Evolved',
        'Y': 'YSO'
    }
}


def classification_results_response(classifier: RandomForestClassifier,
                                    descriptive_classes: Dict[str, str],
                                    predictions: List[List[float]]) -> List[List[str]]:
    results: List[List[str]] = []
    for i, c in enumerate(classifier.classes_):
        results.append([descriptive_classes[c], predictions[0][i]])
    return results


class Classifier:
    def __init__(self):
        self.__gaia_rfc: RandomForestClassifier = load_rfc(ClassificationSource.GAIA)
        self.__2mass_rfc: RandomForestClassifier = load_rfc(ClassificationSource.TWO_MASS)
        self.__wise_rfc: RandomForestClassifier = load_rfc(ClassificationSource.WISE)

    @property
    def gaia_rfc(self) -> RandomForestClassifier:
        return self.__gaia_rfc

    @property
    def twomass_rfc(self) -> RandomForestClassifier:
        return self.__2mass_rfc

    @property
    def wise_rfc(self) -> RandomForestClassifier:
        return self.__wise_rfc

    def classify(self, points: np.array) -> Tuple[List[List[str]], str, int]:
        if points.shape[1] == 3:
            descriptive_classes: Dict[str, str] = CLASSIFIER_LABELS[ClassificationSource.GAIA]
            predictions: List[List[float]] = self.gaia_rfc.predict_proba(points)
            return (classification_results_response(self.gaia_rfc,
                                                    descriptive_classes,
                                                    predictions),
                    'Classified with data from Gaia',
                    ClassificationSource.GAIA.value)
        elif points.shape[1] == 15:
            descriptive_classes: Dict[str, str] = CLASSIFIER_LABELS[ClassificationSource.TWO_MASS]
            predictions: List[List[float]] = self.twomass_rfc.predict_proba(points)
            return (classification_results_response(self.twomass_rfc,
                                                    descriptive_classes,
                                                    predictions),
                    'Classified with data from Gaia and 2MASS',
                    ClassificationSource.TWO_MASS.value)
        elif points.shape[1] == 28:
            descriptive_classes: Dict[str, str] = CLASSIFIER_LABELS[ClassificationSource.WISE]
            predictions: List[List[float]] = self.wise_rfc.predict_proba(points)
            return (classification_results_response(self.wise_rfc,
                                                    descriptive_classes,
                                                    predictions),
                    'Classified with data from Gaia, 2MASS and WISE',
                    ClassificationSource.WISE.value)
        else:
            logger.error('Expecting either 3, 16 or 28 columns')
            return [], 'Not enough data to classify', 0
