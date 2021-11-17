from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response 
from django.http import JsonResponse
# Create your views here.

import sys
import json
sys.path.append('../../spark/')
import findspark
findspark.init()
from pyspark.sql import SparkSession as session
from spark.run_main import get_all_csv ,add_col_total_floor_number_int

class get_data(APIView):

    def get(self,request,*args,**kwds):
        spark = session.builder.master('local').appName('test').config('spark.debug.maxToStringFields', 100).getOrCreate()
        csv = add_col_total_floor_number_int(get_all_csv(spark,'../'))

        get_data = request.GET
        
        city = get_data.get('city')
        floor = get_data.get('floor')
        try:
            if floor:
                floor = int(floor)
        except:
            return Response({"param error":"floor type should be INT ..."})
        building_type = get_data.get('building_type')

        filt_str = self.get_filter(city,floor,building_type)
        if filt_str:
            filted_data = csv.filter(filt_str)
        else:
            return Response({"params error":"params is required , pleaze use '?param=<param>' behind the url"})
        
        filted_data = csv

        json_filted_data = filted_data.toJSON().collect()

        result_list = []
        for _json in json_filted_data:
            result_list.append(json.loads(_json))

        return Response(result_list)

    def get_filter(self,city,floor,building_type):
        filt_str = ""
        if city:
            filt_str += "city == %r" %(city)
        if floor:
            if len(filt_str) != 0:
                filt_str += " and "
            filt_str += " total_floor_number_int == %r" %(floor)
        if building_type:
            if len(filt_str) != 0:
                filt_str += " and "
            filt_str += " building_state == %r" %(building_type)
        return filt_str

