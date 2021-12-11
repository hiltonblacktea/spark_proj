from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
# Create your views here.

import sys
import json
sys.path.append('/spark')

import findspark
findspark.init()
from pyspark.sql import SparkSession as session
from run_main import get_all_csv ,add_col_total_floor_number_int

class get_data(APIView):

    def get(self,request,*args,**kwds):
        """ GET Method
        """
        get_data = request.GET

        # params is required now.
        if len(get_data.keys())==0:
            return Response({"params error":"params is required , please use '?param=<param>' behind the URL"})

        spark , csv = self.get_dataframe()

        city , floor , building_type  = self.get_params(get_data)

        filt_str = self.get_filter(city,floor,building_type)
        filted_data = csv.filter(filt_str)

        json_filted_data = filted_data.toJSON().collect()

        result_list = []
        for _json in json_filted_data:
            result_list.append(json.loads(_json))

        spark.stop()
        return Response(result_list)

    def get_dataframe(self):
        """取得spark連線服務以及取得資料集數據
        Return:
            spark連線服務 , 資料集數據
        """
        spark = session.builder.master('local').appName('test').config('spark.debug.maxToStringFields', 100).getOrCreate()
        return spark , add_col_total_floor_number_int(get_all_csv(spark,'../'))

    def get_filter(self,city,floor,building_type):
        """按照Request參數建立過濾條件
        Parameters:
            city - 鄉鎮市區 , <str>
            floor - 總樓層數 , <int>
            building_type - 建物型態 , <str>
        Return:
            過濾條件字串 , <str>
        """
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

    def get_params(self,get_data):
        """取得Request參數
        Parameters:
            get_data - request.GET , <dict>
        Return:
            參數 city , floor , building_type
        """
        city = get_data.get('city')
        floor = get_data.get('floor')

        try:
            if floor:
                floor = int(floor)
        except:
            return Response({"param error":"floor type should be INT ..."})

        building_type = get_data.get('building_type')
        return city , floor , building_type
