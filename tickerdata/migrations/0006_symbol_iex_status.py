# Generated by Django 3.0.3 on 2020-02-22 03:23

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('tickerdata', '0005_tick_source'),
    ]

    operations = [
        migrations.AddField(
            model_name='symbol',
            name='iex_status',
            field=models.IntegerField(default=200),
        ),
    ]