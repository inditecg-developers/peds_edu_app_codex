from django.contrib import admin
from django.urls import include, path

from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", include("sharing.urls")),
    path("accounts/", include("accounts.urls")),
    path("publisher/", include("publisher.urls")),
]

# Serve media files (doctor photos).
# In production, prefer Nginx, but this ensures it works even if Nginx isn't configured.
urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

# Optional: also serve static if needed (usually WhiteNoise covers /static/)
# urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
