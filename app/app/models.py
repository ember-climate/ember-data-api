from django.db import models

class Area(models.Model):
    name = models.CharField(max_length=255)
    country = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

class FuelType(models.Model):
    name = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

class GlobalGenerationAnnual(models.Model):
    # TODO make these a pre vetted list
    # area = models.ForeignKey(Area, on_delete=models.CASCADE)
    # fueltype = models.ForeignKey(FuelType, on_delete=models.CASCADE)

    year = models.IntegerField()
    area = models.CharField(max_length=255)
    fueltype = models.CharField(max_length=255)
    source = models.CharField(max_length=255, default=None)
    generation_twh = models.FloatField(default=None)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)



    # year,area,fueltype,source,generation_twh
    # column1=year,column2=area,column3=fueltype,column4=source,column5=generation_twh