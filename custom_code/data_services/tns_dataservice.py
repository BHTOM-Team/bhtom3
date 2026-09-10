"""Bounded, name-normalizing adapter for TOM Toolkit's TNS DataService."""

import re
from urllib.parse import quote

import requests

from tom_dataservices.data_services.tns import TNSDataService as BaseTNSDataService

from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT


class TNSDataService(BaseTNSDataService):
    """Use TNS's prefix-free query contract and return the common target result shape."""

    name = 'TNS'

    def build_query_parameters(self, parameters, **kwargs):
        adapted = dict(parameters)
        target_name = str(adapted.get('target_name') or '').strip()
        adapted['target_name'] = re.sub(
            r'^(?:SN|AT)\s*', '', target_name, flags=re.IGNORECASE
        )
        if adapted.get('radius') in (None, '') and adapted.get('radius_arcsec') not in (None, ''):
            adapted['radius'] = adapted['radius_arcsec']
            adapted['units'] = 'arcsec'
        return super().build_query_parameters(adapted, **kwargs)

    def query_service(self, data, **kwargs):
        response = requests.post(
            kwargs['url'],
            data=data,
            headers=self.build_headers(),
            timeout=DATA_SERVICE_HTTP_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        self.query_results = payload.get('data') or []
        return self.query_results

    def query_targets(self, query_parameters):
        targets = super().query_targets(query_parameters)
        for target in targets:
            if not isinstance(target, dict):
                continue
            objname = str(target.get('objname') or '').strip()
            prefix = str(target.get('name_prefix') or '').strip()
            if objname:
                target.setdefault('name', f'{prefix} {objname}'.strip())
                target.setdefault('source_location', f'https://www.wis-tns.org/object/{quote(objname)}')
            target.setdefault('ra', target.get('radeg'))
            target.setdefault('dec', target.get('decdeg'))
        return targets
