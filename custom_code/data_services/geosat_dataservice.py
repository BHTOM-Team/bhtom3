import json
import math
import urllib.request

from sgp4.api import Satrec

from custom_code.geosat import get_tle


class GeoSatDataService:
    name = "GeoSat"
    CELESTRAK_NORAD_JSON_URL = "https://celestrak.org/NORAD/elements/gp.php?CATNR={norad_id}&FORMAT=json"

    @staticmethod
    def _to_float(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def classify_object_type(name: str, object_type: str = ""):
        ot = (object_type or "").strip().upper()
        nm = (name or "").upper()
        if ot:
            if "DEB" in ot:
                return "DEBRIS", True
            if "ROCKET" in ot or "R/B" in ot:
                return "DEBRIS", True
            return "SATELLITE", False
        if " DEB" in nm or nm.endswith("DEB"):
            return "DEBRIS", True
        if " R/B" in nm or "ROCKET BODY" in nm:
            return "DEBRIS", True
        return "SATELLITE", False

    def query_by_norad_id(self, norad_id: int):
        tle = get_tle(norad_id)
        if tle is None:
            raise ValueError(f"Could not resolve TLE for NORAD {norad_id}.")

        tle_name, line1, line2 = tle
        metadata = self.fetch_catalog_record_by_norad(norad_id)
        if metadata:
            object_name = (metadata.get("OBJECT_NAME") or tle_name or f"NORAD {norad_id}").strip()
            object_type, is_debris = self.classify_object_type(object_name, metadata.get("OBJECT_TYPE") or "")
            intldes = (metadata.get("OBJECT_ID") or "").strip()
            source = "celestrak_norad"
        else:
            object_name = (tle_name or f"NORAD {norad_id}").strip()
            object_type, is_debris = self.classify_object_type(object_name, "")
            intldes = ""
            source = "manual"

        sat = Satrec.twoline2rv(line1, line2)

        return {
            "name": object_name,
            "intldes": intldes,
            "source": source,
            "object_type": object_type,
            "is_debris": is_debris,
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

    def fetch_catalog_record_by_norad(self, norad_id: int):
        try:
            url = self.CELESTRAK_NORAD_JSON_URL.format(norad_id=int(norad_id))
            with urllib.request.urlopen(url, timeout=20) as response:
                payload = response.read().decode("utf-8")
            data = json.loads(payload)
            if isinstance(data, list) and data:
                record = data[0]
                if str(record.get("NORAD_CAT_ID", "")).strip() == str(int(norad_id)):
                    return record
        except Exception:
            pass
        return None
