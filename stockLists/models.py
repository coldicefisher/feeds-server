from django.db import models

# Create your models here.
class List(models.Model):
    list_name = models.CharField(max_length=255,unique=True)
    members = models.TextField(blank=True,null=True)
    pull_historical = models.BooleanField(blank=False,default=True)
    pull_live = models.BooleanField(blank=False,default=True)
    pull_options = models.BooleanField(blank=False,default=True)
    
