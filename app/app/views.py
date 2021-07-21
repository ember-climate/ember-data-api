from rest_framework.generics import ListAPIView
from rest_framework import routers, serializers, viewsets
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from .models import GlobalGenerationAnnual

class GlobalGenerationAnnualSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = GlobalGenerationAnnual
        fields = ('year', 'area', 'fueltype', 'source', 'generation_twh', 'created_at', 'updated_at')

class GlobalGenerationAnnualViewSet(viewsets.ModelViewSet):
    serializer_class = GlobalGenerationAnnualSerializer
    permission_classes = [IsAuthenticatedOrReadOnly]
    
    def list(self, request):
        queryset = GlobalGenerationAnnual.objects.all()


class GlobalGenerationAnnualList(ListAPIView):
    serializer_class = GlobalGenerationAnnualSerializer

    def get_queryset(self):
        for f in ['fueltype', 'area', 'year']:
            if self.request.query_params.get(f, None) is not None:
                queryset = GlobalGenerationAnnual.objects.filter(**{f: self.request.query_params.get(f)})
                return queryset