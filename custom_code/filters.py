from django.db.models import Q
from django.contrib import messages

import django_filters

from tom_targets.models import Target, TargetList
from tom_targets.utils import cone_search_filter


def get_target_list_queryset(request):
    if request and request.user.is_authenticated:
        return TargetList.objects.all()
    return TargetList.objects.none()


class BhtomTargetFilterSet(django_filters.FilterSet):
    def filter_name(self, queryset, name, value):
        return queryset.filter(Q(name__icontains=value) | Q(aliases__name__icontains=value)).distinct()

    def filter_description(self, queryset, name, value):
        return queryset.filter(description__icontains=value).distinct()

    def _range_filter(self, queryset, field_name, value):
        if value is None:
            return queryset
        if value.start is not None and value.stop is not None:
            return queryset.filter(**{f"{field_name}__gte": value.start, f"{field_name}__lte": value.stop})
        if value.start is not None:
            return queryset.filter(**{f"{field_name}__gte": value.start})
        if value.stop is not None:
            return queryset.filter(**{f"{field_name}__lte": value.stop})
        return queryset

    def filter_ra(self, queryset, name, value):
        return self._range_filter(queryset, "ra", value)

    def filter_dec(self, queryset, name, value):
        return self._range_filter(queryset, "dec", value)

    def filter_gall(self, queryset, name, value):
        return self._range_filter(queryset, "galactic_lng", value)

    def filter_galb(self, queryset, name, value):
        return self._range_filter(queryset, "galactic_lat", value)

    def filter_importance(self, queryset, name, value):
        return self._range_filter(queryset, "importance", value)

    def filter_sun_distance(self, queryset, name, value):
        return self._range_filter(queryset, "sun_separation", value)

    def filter_mag_last(self, queryset, name, value):
        return self._range_filter(queryset, "mag_last", value)

    def filter_cone_search(self, queryset, name, value):
        if not value:
            return queryset

        if name == "cone_search":
            try:
                ra, dec, radius = [v.strip() for v in value.split(",", 2)]
                ra = float(ra)
                dec = float(dec)
                radius = float(radius)
            except (TypeError, ValueError):
                if getattr(self, "request", None):
                    messages.error(self.request, "Cone Search format: RA, Dec, Radius")
                return queryset.none()
        elif name == "target_cone_search":
            try:
                target_name, radius = [v.strip() for v in value.split(",", 1)]
                radius = float(radius)
            except (TypeError, ValueError):
                if getattr(self, "request", None):
                    messages.error(self.request, "Cone Search (Target) format: Target Name, Radius")
                return queryset.none()

            targets = Target.objects.filter(
                Q(name__icontains=target_name) | Q(aliases__name__icontains=target_name)
            ).distinct()

            if len(targets) != 1:
                return queryset.none()

            ra = targets[0].ra
            dec = targets[0].dec
        else:
            return queryset

        return cone_search_filter(queryset, ra, dec, radius)

    name = django_filters.CharFilter(method="filter_name", label="Name")
    description = django_filters.CharFilter(method="filter_description", label="Description")

    cone_search = django_filters.CharFilter(
        method="filter_cone_search",
        label="Cone Search",
        help_text="RA, Dec, Search Radius (degrees)",
    )
    target_cone_search = django_filters.CharFilter(
        method="filter_cone_search",
        label="Cone Search (Target)",
        help_text="Target Name, Search Radius (degrees)",
    )

    ra = django_filters.RangeFilter(method="filter_ra", label="RA")
    dec = django_filters.RangeFilter(method="filter_dec", label="Dec")
    gall = django_filters.RangeFilter(method="filter_gall", label="Galactic Longitude (0,360)")
    galb = django_filters.RangeFilter(method="filter_galb", label="Galactic Latitude (-90,90)")

    importance = django_filters.RangeFilter(method="filter_importance", label="Importance (0,10)")
    sun = django_filters.RangeFilter(method="filter_sun_distance", label="Sun separation")
    mag = django_filters.RangeFilter(method="filter_mag_last", label="Last magnitude")

    type = django_filters.ChoiceFilter(choices=Target.TARGET_TYPES)
    targetlist__name = django_filters.ModelChoiceFilter(
        queryset=get_target_list_queryset,
        label="Target Group",
    )

    classification = django_filters.ChoiceFilter(
        choices=Target._meta.get_field("classification").choices,
        label="Classification",
    )

    class Meta:
        model = Target
        fields = [
            "name",
            "description",
            "type",
            "classification",
            "cone_search",
            "target_cone_search",
            "ra",
            "dec",
            "gall",
            "galb",
            "importance",
            "sun",
            "mag",
            "targetlist__name",
        ]
