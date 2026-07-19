import django.db.models.deletion
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):

    dependencies = [
        ("contentcuration", "0169_invitation_organization"),
    ]

    operations = [
        migrations.AddField(
            model_name="change",
            name="organization",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                to="contentcuration.organization",
            ),
        ),
    ]
