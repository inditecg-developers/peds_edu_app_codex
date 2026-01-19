from django.contrib import admin
from django.urls import include, path

from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path("admin/", admin.site.urls),

    # SSO consume endpoint (Project2 destination)
    path("sso/", include("sso.urls")),

    # Campaign publishing module routes at root:
    # /publisher-landing-page/, /add-campaign-details/, /campaigns/, /publisher-api/...
    path("", include(("publisher.campaign_urls", "campaign_publisher"), namespace="campaign_publisher")),

    # Existing app routes
    path("", include("sharing.urls")),
    path("accounts/", include("accounts.urls")),
    path("publisher/", include("publisher.urls")),
]

# Serve uploaded media (doctor photos + campaign banners)
urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
