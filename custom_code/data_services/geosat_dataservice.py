import math

from sgp4.api import Satrec

from custom_code.geosat import get_tle


class GeoSatDataService:
    name = "GeoSat"

    @staticmethod
    def _to_float(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def query_by_norad_id(self, norad_id: int):
        tle = get_tle(norad_id)
        if tle is None:
            raise ValueError(f"Could not resolve TLE for NORAD {norad_id}.")

        tle_name, line1, line2 = tle
        sat = Satrec.twoline2rv(line1, line2)

        return {
            "name": (tle_name or f"NORAD {norad_id}").strip(),
            "tle_name": (tle_name or "").strip(),
            "tle_line1": line1,
            "tle_line2": line2,
            "epoch_jd": self._to_float(sat.jdsatepoch + sat.jdsatepochF),
            "inclination_deg": self._to_float(math.degrees(sat.inclo)),
            "eccentricity": self._to_float(sat.ecco),
            "raan_deg": self._to_float(math.degrees(sat.nodeo)),
            "arg_perigee_deg": self._to_float(math.degrees(sat.argpo)),
            "mean_anomaly_deg": self._to_float(math.degrees(sat.mo)),
            "mean_motion_rev_per_day": self._to_float(sat.no_kozai * 1440.0 / (2.0 * math.pi)),
            "bstar": self._to_float(sat.bstar),
        }
