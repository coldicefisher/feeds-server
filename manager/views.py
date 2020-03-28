import sys
sys.path.append("..") # Adds higher directory to python modules path.

from django.shortcuts import render
from django.http import HttpResponse
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.renderers import JSONRenderer
from datetime import datetime as dt
import feedEngine
from feedEngine import feed_tasks as tasks
# Create your views here.

# dict_response = {'task':'get','timestamp':dt.strftime(dt.now(),'%Y-%m-%d %H:%M')}

class GetHistoricalDataView(APIView):
    renderer_classes = [JSONRenderer] # this tells DRF to render in JSON. Not use the model viewer.
    
    def get(self,request):
        #wait = self.request.query_params.get('wait',None)
        dict_response = {'task':'get_historical_data','timestamp':dt.strftime(dt.now(),'%Y-%m-%d %H:%M')}
        dict_response['value']  = tasks.get_historical_data()
        return Response(dict_response)

        # elif command == 'get_stock_members': 
        #     tasks.get_stock_members.delay()
        #     dict_response['value'] = 'queued'

        # elif command == 'get_options_data': 
        #     tasks.get_options_data.delay()
        #     dict_response['value'] = 'queued'

class GetNumOfWorkers(APIView):
    renderer_classes = [JSONRenderer] # this tells DRF to render in JSON. Not use the model viewer.

    def get(self,request):
        dict_response = {'task':'number_of_workers','timestamp':dt.strftime(dt.now(),'%Y-%m-%d %H:%M')}
        dict_response['value'] = tasks.num_of_workers()
        return Response(dict_response)

class StartWorker(APIView):
    renderer_classes = [JSONRenderer] # this tells DRF to render in JSON. Not use the model viewer.

    def get(self,request):
        dict_response = {'task':'start_worker','timestamp':dt.strftime(dt.now(),'%Y-%m-%d %H:%M')}
        num = tasks.start_worker()
        dict_response['value'] = num
        return Response(dict_response)

class StartFlower(APIView):
    renderer_classes = [JSONRenderer] # this tells DRF to render in JSON. Not use the model viewer.
    
    def get(self,request):
        dict_response = {'task':'start_flower','timestamp':dt.strftime(dt.now(),'%Y-%m-%d %H:%M')}
        dict_response['value'] = tasks.start_flower()
        return Response(dict_response)

class BackupDB(APIView):
    renderer_classes = [JSONRenderer] # this tells DRF to render in JSON. Not use the model viewer.

    def get(self,request):
        dict_response = {'task':'backup_db','timestamp':dt.strftime(dt.now(),'%Y-%m-%d %H:%M')}
        dict_response['value'] = tasks.backup_db()
        return Response(dict_response)
