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

from custom_code.views import (
    BhtomCatalogQueryView,
    Bhtom2TargetListView,
    BhtomTargetCreateView,
    BhtomTargetDetailView,
    BhtomTargetUpdateView,
    GeoTomAddSatView,
    GeoTomDeleteSatView,
    GeoTomRefreshTleView,
    GeoTomRefreshSingleTleView,
    GeoTomTargetListView,
    LegacyLogoutView,
    UpdateReducedDataAndDataServicesView,
)

urlpatterns = [
    path('accounts/logout/', LegacyLogoutView.as_view(), name='logout'),
    path('catalogs/query/', BhtomCatalogQueryView.as_view(), name='catalog-query-override'),
    path('targets/', Bhtom2TargetListView.as_view(), name='targets-list-override'),
    path('targets/create/', BhtomTargetCreateView.as_view(), name='targets-create-override'),
    path('targets/<int:pk>/update/', BhtomTargetUpdateView.as_view(), name='targets-update-override'),
    path('targets/<int:pk>/', BhtomTargetDetailView.as_view(), name='targets-detail-override'),
    path('geotom/', GeoTomTargetListView.as_view(), name='geotom-list'),
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
