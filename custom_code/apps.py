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
            {'class': f'{self.name}.data_services.desi_dataservice.DESIDataService'}
        ]
