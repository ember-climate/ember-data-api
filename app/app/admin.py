from django.contrib import admin
from .models import GlobalGenerationAnnual

@admin.register(GlobalGenerationAnnual)
class GlobalGenerationAnnualAdmin(admin.ModelAdmin):
    list_display = ('year', 'area', 'fueltype', 'source', 'generation_twh', 'created_at', 'updated_at')
    list_filter = ('year', 'area', 'fueltype', 'source', 'generation_twh', 'created_at', 'updated_at')
    search_fields = ('year', 'area', 'fueltype', 'source')
