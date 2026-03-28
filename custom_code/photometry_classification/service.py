import logging
from functools import lru_cache
from typing import Optional

from .calculate_colors import colors_df
from .catalog_query import query_for_object
from .classifier import Classifier


logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_classifier() -> Classifier:
    return Classifier()


def classify_target_coordinates(ra: Optional[float], dec: Optional[float]) -> str:
    if ra is None or dec is None:
        return "-"

    photometry = query_for_object(float(ra), float(dec))
    if photometry.empty:
        return "--"

    colors = colors_df(photometry)
    if colors.empty:
        return "--"

    predictions, _message, _classifier_index = get_classifier().classify(colors)
    if not predictions:
        return "--"

    best_name = ""
    best_confidence = -1.0
    for class_name, confidence in predictions:
        try:
            numeric_confidence = float(confidence)
        except (TypeError, ValueError):
            continue
        if numeric_confidence > best_confidence:
            best_name = str(class_name)
            best_confidence = numeric_confidence

    if not best_name or best_confidence < 0:
        return "--"

    return f"{best_name} {best_confidence:.1%}"
