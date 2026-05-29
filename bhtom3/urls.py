"""django URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.urls import path, include
from rest_framework.authtoken.views import obtain_auth_token

from custom_code.views import (
    BhtomCatalogSelectResultView,
    BhtomCatalogQueryView,
    BhtomPallasAView,
    Bhtom2TargetListView,
    BhtomTargetCreateView,
    BhtomTargetDetailView,
    TargetDownloadPhotometryDataApiView,
    BhtomTargetUpdateView,
    GenericTargetSearchRedirectView,
    GeoTomAddSatView,
    BhtomPallasEphemerisView,
    BhtomPallasPhotometryView,
    BhtomPallasVisibleView,
    BhtomPallasView,
    GeoTomDeleteSatView,
    GeoTomLiveDataView,
    GeoTomRefreshTleView,
    GeoTomRefreshSingleTleView,
    GeoTomTargetListView,
    LegacyLogoutView,
    ProposalListView,
    FacilityAccountCreateView,
    FacilityAccountUpdateView,
    FacilityAccountDeleteView,
    FacilityAccountSyncProposalsView,
    LCOProposalImportView,
    FacilityProposalCreateView,
    FacilityProposalUpdateView,
    FacilityProposalDeleteView,
    UserCreateWithFixedFormView,
    UserProfileRedirectView,
    UserUpdateWithTokenView,
    UpdateReducedDataAndDataServicesView,
    TargetPeriodicityView,
    TargetPeriodicityComputeView,
)

urlpatterns = [
    path('accounts/logout/', LegacyLogoutView.as_view(), name='logout'),
    path('api/token-auth/', obtain_auth_token, name='api-token-auth'),
    path('dataservices/', include(('custom_code.dataservices_urls', 'dataservices'), namespace='dataservices')),
    path('catalogs/query/', BhtomCatalogQueryView.as_view(), name='catalog-query-override'),
    path('catalogs/query/select/', BhtomCatalogSelectResultView.as_view(), name='catalog-select-result'),
    path('targets/search/', GenericTargetSearchRedirectView.as_view(), name='targets-generic-search'),
    path('targets/', Bhtom2TargetListView.as_view(), name='targets-list-override'),
    path('targets/download-photometry/', TargetDownloadPhotometryDataApiView.as_view(), name='targets-download-photometry-api'),
    path('proposals/', ProposalListView.as_view(), name='proposal-list'),
    path('proposals/lco/import/', LCOProposalImportView.as_view(), name='proposal-import-lco'),
    path('proposals/<str:facility_code>/add/', FacilityProposalCreateView.as_view(), name='proposal-create'),
    path('proposals/<str:facility_code>/accounts/add/', FacilityAccountCreateView.as_view(), name='proposal-account-create'),
    path('proposals/accounts/<int:pk>/edit/', FacilityAccountUpdateView.as_view(), name='proposal-account-update'),
    path('proposals/accounts/<int:pk>/delete/', FacilityAccountDeleteView.as_view(), name='proposal-account-delete'),
    path('proposals/accounts/<int:pk>/sync/', FacilityAccountSyncProposalsView.as_view(), name='proposal-account-sync'),
    path('proposals/items/<int:pk>/edit/', FacilityProposalUpdateView.as_view(), name='proposal-update'),
    path('proposals/items/<int:pk>/delete/', FacilityProposalDeleteView.as_view(), name='proposal-delete'),
    path('users/profile/', UserProfileRedirectView.as_view(), name='user-profile'),
    path('users/create/', UserCreateWithFixedFormView.as_view(), name='user-create'),
    path('users/<int:pk>/update/', UserUpdateWithTokenView.as_view(), name='user-update'),
    path('targets/create/', BhtomTargetCreateView.as_view(), name='targets-create-override'),
    path('targets/<int:pk>/update/', BhtomTargetUpdateView.as_view(), name='targets-update-override'),
    path('targets/<int:pk>/', BhtomTargetDetailView.as_view(), name='targets-detail-override'),
    path('targets/<int:pk>/models/periodicity/', TargetPeriodicityView.as_view(), name='target-periodicity'),
    path('targets/<int:pk>/models/periodicity/compute/', TargetPeriodicityComputeView.as_view(), name='target-periodicity-compute'),
    path('geotom/', GeoTomTargetListView.as_view(), name='geotom-list'),
    path('geotom/live-data/', GeoTomLiveDataView.as_view(), name='geotom-live-data'),
    path('bhtom-pallas/', BhtomPallasView.as_view(), name='bhtom-pallas'),
    path('bhtom-pallas/visible/', BhtomPallasVisibleView.as_view(), name='bhtom-pallas-visible'),
    path('bhtom-pallas/photometry/', BhtomPallasPhotometryView.as_view(), name='bhtom-pallas-photometry'),
    path('bhtom-pallas/ephemeris/', BhtomPallasEphemerisView.as_view(), name='bhtom-pallas-ephemeris'),
    path('bhtom-pallas/a/', BhtomPallasAView.as_view(), name='bhtom-pallas-a'),
    path('geotom/add/', GeoTomAddSatView.as_view(), name='geotom-add-sat'),
    path('geotom/refresh-tle/', GeoTomRefreshTleView.as_view(), name='geotom-refresh-tle'),
    path('geotom/<int:pk>/refresh-tle/', GeoTomRefreshSingleTleView.as_view(), name='geotom-refresh-single-tle'),
    path('geotom/<int:pk>/delete/', GeoTomDeleteSatView.as_view(), name='geotom-delete-sat'),
    path(
        'dataproducts/data/reduced/update/',
        UpdateReducedDataAndDataServicesView.as_view(),
        name='update-reduced-data-services',
    ),
    path('', include('tom_common.urls')),
]
