from enum import Enum
from pathlib import Path
from typing import Dict, List

BASE_DIR = Path(__file__).resolve().parent


class ClassificationSource(Enum):
    GAIA = 1
    TWO_MASS = 2
    WISE = 3


SELECTED_FEATURES: Dict[ClassificationSource, List[str]] = {
    ClassificationSource.GAIA: ['bpmag-Gmag', 'bpmag-rpmag', 'Gmag-rpmag'],
    ClassificationSource.WISE: ['bpmag-Gmag', 'bpmag-rpmag', 'bpmag-Jmag', 'bpmag-Hmag', 'bpmag-Kmag', 'bpmag-W1mag',
                               'bpmag-W2mag', 'Gmag-rpmag', 'Gmag-Jmag', 'Gmag-Hmag', 'Gmag-Kmag', 'Gmag-W1mag',
                               'Gmag-W2mag',
                               'rpmag-Jmag', 'rpmag-Hmag', 'rpmag-Kmag', 'rpmag-W1mag', 'rpmag-W2mag', 'Jmag-Hmag',
                               'Jmag-Kmag',
                               'Jmag-W1mag', 'Jmag-W2mag', 'Hmag-Kmag', 'Hmag-W1mag', 'Hmag-W2mag', 'Kmag-W1mag',
                               'Kmag-W2mag',
                               'W1mag-W2mag'],
    ClassificationSource.TWO_MASS: ["bpmag-Gmag", "bpmag-rpmag", "bpmag-Jmag", "bpmag-Hmag", "bpmag-Kmag", "Gmag-rpmag",
                                   "Gmag-Jmag", "Gmag-Hmag", "Gmag-Kmag", "rpmag-Jmag", "rpmag-Hmag", "rpmag-Kmag",
                                   "Jmag-Hmag", "Jmag-Kmag", "Hmag-Kmag"]
}

TRAINING_FILE_PATHS: Dict[ClassificationSource, str] = {
    ClassificationSource.GAIA: str(BASE_DIR / "data" / "training_gaia.csv"),
    ClassificationSource.WISE: str(BASE_DIR / "data" / "training_WISE.csv"),
    ClassificationSource.TWO_MASS: str(BASE_DIR / "data" / "training_2MASS.csv")
}

MODEL_PATHS: Dict[ClassificationSource, str] = {
    ClassificationSource.GAIA: str(BASE_DIR / "models" / "gaia_rfc.pickle"),
    ClassificationSource.WISE: str(BASE_DIR / "models" / "wise_rfc.pickle"),
    ClassificationSource.TWO_MASS: str(BASE_DIR / "models" / "2mass_rfc.pickle")
}
