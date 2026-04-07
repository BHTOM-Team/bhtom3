from django.apps import AppConfig


class CustomCodeConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "custom_code"

    def data_services(self):
        return [
            {'class': f'{self.name}.data_services.crts_dataservice.CRTSDataService'},
            {'class': f'{self.name}.data_services.sdss_dataservice.SDSSDataService'},
            {'class': f'{self.name}.data_services.gaia_alerts_dataservice.GaiaAlertsDataService'},
            {'class': f'{self.name}.data_services.ogle_ews_dataservice.OGLEEWSDataService'},
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
            {'class': f'{self.name}.data_services.neowise_dataservice.NeoWISEDataService'},
            {'class': f'{self.name}.data_services.simbad_dataservice.SimbadDataService'},
            {'class': f'{self.name}.data_services.photometric_classification_dataservice.PhotometricClassificationDataService'},
            {'class': f'{self.name}.data_services.ptf_dataservice.PTFDataService'},
            {'class': f'{self.name}.data_services.lco_spec_dataservice.LCOSpectraDataService'},
        ]

    def ready(self):
        # Import local signal handlers.
        from . import signals  # noqa: F401

        # Some TOM Toolkit versions have brittle signal receivers.
        try:
            from django.db.models.signals import pre_save
            from django.contrib.auth import get_user_model
            from django.contrib.auth.signals import user_logged_in
            from tom_common import signals as tom_common_signals

            login_receiver = getattr(tom_common_signals, 'set_cipher_on_user_logged_in', None)
            if login_receiver is not None:
                user_logged_in.disconnect(receiver=login_receiver)

            pre_save_receiver = getattr(tom_common_signals, 'user_updated_on_user_pre_save', None)
            if pre_save_receiver is not None:
                pre_save.disconnect(receiver=pre_save_receiver, sender=get_user_model())
        except Exception:
            # If this TOM version does not expose these receivers, nothing to do.
            pass
