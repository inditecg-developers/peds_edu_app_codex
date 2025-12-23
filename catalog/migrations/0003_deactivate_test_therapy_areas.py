from __future__ import annotations

from django.db import migrations


def deactivate_test_therapy_areas(apps, schema_editor):
    TherapyArea = apps.get_model("catalog", "TherapyArea")

    # Mark obvious test rows inactive (so they disappear from publisher screens).
    TherapyArea.objects.filter(code__istartswith="TEST").update(is_active=False)
    TherapyArea.objects.filter(display_name__istartswith="Test").update(is_active=False)


class Migration(migrations.Migration):

    dependencies = [
        ("catalog", "0002_remove_video_duration_seconds"),
    ]

    operations = [
        migrations.RunPython(deactivate_test_therapy_areas, migrations.RunPython.noop),
    ]
