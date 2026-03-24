from django.apps import AppConfig


class CustomCodeConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "custom_code"

    def data_services(self):
        return [
            {'class': f'{self.name}.data_services.crts_dataservice.CRTSDataService'},
            {'class': f'{self.name}.data_services.gaia_alerts_dataservice.GaiaAlertsDataService'},
            {'class': f'{self.name}.data_services.gaia_dr3_dataservice.GaiaDR3DataService'},
            {'class': f'{self.name}.data_services.lsst_dataservice.LSSTDataService'},
            {'class': f'{self.name}.data_services.skymapper_dataservice.SkyMapperDataService'},
            {'class': f'{self.name}.data_services.swiftuvot_dataservice.SwiftUVOTDataService'},
            {'class': f'{self.name}.data_services.galex_dataservice.GalexDataService'},
            {'class': f'{self.name}.data_services.gs6df_dataservice.Gs6dfDataService'},
            {'class': f'{self.name}.data_services.desi_dataservice.DESIDataService'},
            {'class': f'{self.name}.data_services.asassn_dataservice.ASASSNDataService'},
            {'class': f'{self.name}.data_services.panstarrs_dataservice.PanSTARRSDataService'},
            {'class': f'{self.name}.data_services.allwise_dataservice.AllWISEDataService'},
            {'class': f'{self.name}.data_services.neowise_dataservice.NeoWISEDataService'}
        ]

    def ready(self):
        # Import local signal handlers.
        from . import signals  # noqa: F401

        # Some TOM Toolkit versions have a brittle login signal receiver that assumes
        # request.POST["password"] is always present. Disconnect it and use our safe one.
        try:
            from django.contrib.auth.signals import user_logged_in
            from tom_common import signals as tom_common_signals

            receiver = getattr(tom_common_signals, 'set_cipher_on_user_logged_in', None)
            if receiver is not None:
                user_logged_in.disconnect(receiver=receiver)
        except Exception:
            # If this TOM version does not expose that receiver, nothing to do.
            pass
