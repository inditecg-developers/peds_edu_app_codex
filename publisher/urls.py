from django.urls import path
from . import views

app_name = "publisher"

urlpatterns = [
    path("", views.dashboard, name="dashboard"),

    # Therapy Areas
    path("therapy-areas/", views.therapy_list, name="therapy_list"),
    path("therapy-areas/new/", views.therapy_create, name="therapy_create"),
    path("therapy-areas/<int:pk>/", views.therapy_edit, name="therapy_edit"),

    # Trigger Clusters
    path("trigger-clusters/", views.trigger_cluster_list, name="triggercluster_list"),
    path("trigger-clusters/new/", views.trigger_cluster_create, name="triggercluster_create"),
    path("trigger-clusters/<int:pk>/", views.trigger_cluster_edit, name="triggercluster_edit"),

    # Triggers
    path("triggers/", views.trigger_list, name="trigger_list"),
    path("triggers/new/", views.trigger_create, name="trigger_create"),
    path("triggers/<int:pk>/", views.trigger_edit, name="trigger_edit"),

    # Videos
    path("videos/", views.video_list, name="video_list"),
    path("videos/new/", views.video_create, name="video_create"),
    path("videos/<int:pk>/", views.video_edit, name="video_edit"),

    # Bundles
    path("bundles/", views.cluster_list, name="cluster_list"),
    path("bundles/new/", views.cluster_create, name="cluster_create"),
    path("bundles/<int:pk>/", views.cluster_edit, name="cluster_edit"),

    # Trigger Maps
    path("trigger-maps/", views.map_list, name="map_list"),
    path("trigger-maps/new/", views.map_create, name="map_create"),
    path("trigger-maps/<int:pk>/", views.map_edit, name="map_edit"),
]
