from django.urls import path

from . import campaign_views

app_name = "campaign_publisher"

urlpatterns = [
    path(
        "publisher-landing-page/",
        campaign_views.publisher_landing_page,
        name="publisher_landing_page",
    ),
    path(
        "add-campaign-details/",
        campaign_views.add_campaign_details,
        name="add_campaign_details",
    ),
    path(
        "campaigns/",
        campaign_views.campaign_list,
        name="campaign_list",
    ),
    path(
        "campaigns/<str:campaign_id>/edit/",
        campaign_views.edit_campaign_details,
        name="edit_campaign_details",
    ),
    # APIs for search + selection expansion
    path(
        "publisher-api/search/",
        campaign_views.api_search_catalog,
        name="api_search_catalog",
    ),
    path(
        "publisher-api/expand-selection/",
        campaign_views.api_expand_selection,
        name="api_expand_selection",
    ),
    path("field-rep-landing-page/", campaign_views.field_rep_landing_page, name="field_rep_landing_page"),

]
