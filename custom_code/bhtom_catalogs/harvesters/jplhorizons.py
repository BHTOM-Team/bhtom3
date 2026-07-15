import re

from astroquery.jplhorizons import Horizons
from tom_catalogs.harvester import AbstractHarvester

JPL_NON_SIDEREAL_DESCRIPTION = 'Non-sidereal object from JPL'
SOLAR_SYSTEM_CLASSIFICATION = 'SSO'


def _normalize_query(term):
    normalized = ' '.join(str(term or '').strip().split())
    normalized = normalized.replace('\u2013', '-').replace('\u2014', '-')
    return re.sub(r'\s*/\s*', '/', normalized)


def _is_ambiguity(message):
    return 'ambiguous target' in str(message).lower()


def _latest_record_from_ambiguity(message):
    latest = None
    for line in str(message).splitlines():
        match = re.match(r'^\s*(\d+)\s+(\d{4})\s+', line)
        if not match:
            continue
        record = match.group(1)
        epoch_year = int(match.group(2))
        if latest is None or (epoch_year, record) > latest:
            latest = (epoch_year, record)
    return latest[1] if latest else ''


def _target_attempts(term):
    normalized = _normalize_query(term)
    attempts = []

    def add(identifier, id_type):
        candidate = str(identifier or '').strip()
        key = (candidate, id_type)
        if candidate and key not in attempts:
            attempts.append(key)

    add(normalized, None)
    add(normalized, 'smallbody')

    numbered_comet = re.match(r'^(\d+[PD])(?:/(.+))?$', normalized, re.IGNORECASE)
    if numbered_comet:
        designation = numbered_comet.group(1).upper()
        name = (numbered_comet.group(2) or '').strip()
        add(designation, 'designation')
        add(designation, 'smallbody')
        add(designation, None)
        if name:
            add(name, 'comet_name')
            add(name, 'name')

    if re.match(r'^[A-Za-z][A-Za-z0-9 .()_-]*$', normalized):
        add(normalized, 'comet_name')
        add(normalized, 'asteroid_name')
        add(normalized, 'name')

    return attempts


class JPLHorizonsHarvester(AbstractHarvester):
    """
    BHTOM wrapper for JPL Horizons that handles numbered periodic comet aliases.

    Horizons treats identifiers such as ``80P`` as ambiguous apparition records,
    while ``80P/Peters-Hartley`` is not accepted as a direct identifier. For the
    catalog lookup flow we choose the newest record returned by Horizons.
    """

    name = 'JPL Horizons'

    def query(self, term, location=None, start=None, end=None, step=None):
        if all((start, end, step)):
            epochs = {'start': start, 'stop': end, 'step': step}
        else:
            epochs = None

        self.catalog_data = {}
        ambiguous_error = None

        for identifier, id_type in _target_attempts(term):
            try:
                self.catalog_data = Horizons(
                    id=identifier,
                    id_type=id_type,
                    location=location,
                    epochs=epochs,
                ).elements()
                return self.catalog_data
            except (ValueError, IOError) as exc:
                if _is_ambiguity(exc) and ambiguous_error is None:
                    ambiguous_error = exc

        record = _latest_record_from_ambiguity(ambiguous_error) if ambiguous_error else ''
        if record:
            try:
                self.catalog_data = Horizons(
                    id=record,
                    id_type=None,
                    location=location,
                    epochs=epochs,
                ).elements()
            except (ValueError, IOError):
                self.catalog_data = {}
        return self.catalog_data

    def to_target(self):
        target = super().to_target()
        target.type = 'NON_SIDEREAL'
        target.scheme = 'MPC_MINOR_PLANET'
        target.classification = SOLAR_SYSTEM_CLASSIFICATION
        target.description = JPL_NON_SIDEREAL_DESCRIPTION
        target.name = str(self.catalog_data['targetname'][0])
        target.mean_anomaly = self.catalog_data['M'][0]
        target.arg_of_perihelion = self.catalog_data['w'][0]
        target.lng_asc_node = self.catalog_data['Omega'][0]
        target.inclination = self.catalog_data['incl'][0]
        target.mean_daily_motion = self.catalog_data['n'][0]
        target.semimajor_axis = self.catalog_data['a'][0]
        target.eccentricity = self.catalog_data['e'][0]
        target.epoch_of_elements = self.jd_to_mjd(self.catalog_data['datetime_jd'][0])
        target.epoch_of_perihelion = self.jd_to_mjd(self.catalog_data['Tp_jd'][0])
        target.perihdist = self.catalog_data['q'][0]
        target.ephemeris_period = self.catalog_data['P'][0]
        return target
