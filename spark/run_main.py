# -*- utf-8 -*-
import findspark
import json
import os
import collections
from random import randint
from change_cn2an import cn2an
# 在python cmd 無法正常 -> init內帶入spark檔案位置()
findspark.init()

from pyspark.sql import SparkSession as session
from pyspark.sql.functions import when , lit , desc

json_file = ['result-part1.json','result-part2.json']
TARGET_FILES = ['A_lvr_land_A.csv','B_lvr_land_A.csv','E_lvr_land_A.csv','F_lvr_land_A.csv','H_lvr_land_A.csv']

def check_files_exist(target_files):
    """確認資料集皆存在
    Parameters:
        target_files - 保存資料集之資料夾
    Return:
        boolean , True -> 資料集皆存在 False -> 資料集有缺陷 
    """
    sign = True
    if not os.path.exists(target_files):
        print('data dir is not exist...\n')
        return False
    data_file_list = os.listdir(target_files)
    for data in TARGET_FILES:
        if data not in data_file_list:
            print(data+' not in file!!!\n')
            sign = False
    return sign
### check_files_exist

def format_date(date):
    """將交易年月日修改成規定format <%Y-%m-%d>
    Parameters:
        date - 交易年月日 , <str> , '1070103'
    Return:
        formatted date , <str>
    """
    day = date[-2:]
    month = date[-4:-2]
    year = date[:-4]
    
    CE_year = str(int(year) + 1911)
    
    return CE_year + '-' + month + '-' + day
### format_date

def classify_data_by_city(data,result_dict):
    """逐筆資料讀入，整理出以城市為分類的dict，按照以下格式存入dict
    Parameters:
        data - 從數據集中取出之一筆數據 , <dict> , {'city':'台北市','type':'其他',...}
        result_dict - 整理後儲存之dict , <dict> , 格式如下
    Return:
        以城市分類之字典檔 , <dict>
    """
    """ 
    result_dict = 
        {'台北市':{
                    'city'       :   '台北市',
                    'time_slot'  :   [
                                      {
                                         'date':'2018-12-22',
                                         'events':[{'type':'其他','district':'清水區'}]
                                      } ,
                                      {'date':...},
                                     ]
                  }
        },
         '台中市':{...}
        }
    """
    city = data.get('city')
    
    orig_date = data.get('transaction_year_month_and_day')

    date = format_date(orig_date)
    time_slots_data = {'date':date}

    events = []
    tmp_dic = {}
    for key , value in data.items():
        if key in ['building_state','The_villages_and_towns_urban_district']:
            tmp_dic[key] = value
    events.append(tmp_dic)

    time_slots_data['events'] = events

    # 整理的字典檔keys沒有該city -> 第一筆city數據
    if city not in result_dict.keys():
        result_dict[city] = collections.OrderedDict()
        result_dict[city]['city'] = city
        result_dict[city]['time_slots'] = [time_slots_data]
    else:
        result_dict[city]['time_slots'].append(time_slots_data)

    return result_dict
### classify_data_by_city


def rename_col(csv):
    """將col schema name替換成英文(根據第1筆數據資料做處理)
    Parameter:
        csv - dataframe 物件資料 , <pyspark.sql.dataframe.DataFrame>
    Return:
        csv - dataframe 物件資料 , <pyspark.sql.dataframe.DataFrame>
    """
    last_five_title = ['Main_building_area','Attached_building_area','Balcony_area','elevator','Transfer_number']
    eng_title = csv.head(1)[0]
    rename_list = []

    for title in eng_title:
        if title is not None:
            # 以 "_" 連接 方便後續使用
            title = title.replace(' ','_')
            rename_list.append(title)
    rename_list.extend(last_five_title)

    old_shema = csv.schema.names
    for index in range(len(old_shema)):
        csv = csv.withColumnRenamed(old_shema[index],rename_list[index])
    
    return csv
### rename_col

def get_all_csv(spark,adjust_path):
    """透過spark讀取所有資料集，每個資料集加上所在城市，最後整合成一份dataframe
    Parameters:
        spark - pyspark連線物件 ,  <pyspark.sql.session.SparkSession>
        adjust_path - 依相對路徑方法調整至crawler位置 , <str> ,'../'
    Return:
        csv - 所有資料集合併完成之 dataframe 物件資料 , <pyspark.sql.dataframe.DataFrame>
    """
    orig_csv = spark.read.csv(adjust_path + 'crawler/data/A_lvr_land_A.csv',header=True)
    rename_col_csv = rename_col(orig_csv)
    csv = rename_col_csv.withColumn("city",lit("台北市"))

    orig_csv2 = spark.read.csv(adjust_path + 'crawler/data/B_lvr_land_A.csv',header=True)
    rename_col_csv2 = rename_col(orig_csv2)
    csv2=rename_col_csv2.withColumn("city",lit("臺中市"))

    orig_csv3 = spark.read.csv(adjust_path + 'crawler/data/E_lvr_land_A.csv',header=True)
    rename_col_csv3 = rename_col(orig_csv3)
    csv3 = rename_col_csv3.withColumn("city",lit("高雄市"))

    orig_csv4 = spark.read.csv(adjust_path + 'crawler/data/F_lvr_land_A.csv',header=True)
    rename_col_csv4 = rename_col(orig_csv4)
    csv4 = rename_col_csv4.withColumn("city",lit("新北市"))

    orig_csv5 = spark.read.csv(adjust_path + 'crawler/data/H_lvr_land_A.csv',header=True)
    rename_col_csv5 = rename_col(orig_csv5)
    csv5 = rename_col_csv5.withColumn("city",lit("桃園市"))

    csv = csv.union(csv2)
    csv = csv.union(csv3)
    csv = csv.union(csv4)
    csv = csv.union(csv5)
    # 加入數字總樓層 預設 0
    csv = csv.withColumn("total_floor_number_int",lit(0))
    return csv
### get_all_csv

def add_col_total_floor_number_int(csv):
    """新增一欄位儲存 對應總樓層數之數字版本 , 為後續filter整理<filter=欲取得總樓層數大於等於十三層>
    Parameters:
        csv - dataframe 物件資料 , <pyspark.sql.dataframe.DataFrame>
    Return:
        csv - 調整完成之 dataframe 物件資料 , <pyspark.sql.dataframe.DataFrame>
    """
    total_floor = csv.select('total_floor_number').distinct().collect()
    
    for floor in total_floor:
        floor = floor.total_floor_number
        if floor:
            if floor[-1] != '層':
                continue
            int_floor = cn2an(floor[0:-1],)
            csv = csv.withColumn('total_floor_number_int',when(csv.total_floor_number==floor,int_floor).otherwise(csv.total_floor_number_int))
    return csv
### change_data_for_filter

def main():
    # 確認資料集完整性
    if False == check_files_exist('/crawler/data'):
        print (','.join(TARGET_FILES)+' is required')
        exit('please crawler data first by using /crawler/run_main.py ')
    
    # 連線spark,取得所有資料集數據
    spark = session.builder.master('local').appName('test').config('spark.debug.maxToStringFields', 100).getOrCreate()
    csv =  add_col_total_floor_number_int(get_all_csv(spark,'../'))

    # 過濾出符合 1.總樓層數大於等於13層 2. 主要用途為<住家用> 3. 建物類型為<住宅大樓>(模糊查詢)
    filted_data = csv.filter((csv['total_floor_number_int']>=13) & (csv['main_use']=='住家用') & (csv['building_state'].like('住宅大樓%')))
    
    # 降冪排列
    desc_filted_data = filted_data.sort(desc('transaction_year_month_and_day'))
    
    # 將dataframe轉換成json格式
    json_res_list = desc_filted_data.toJSON().collect()

    # 逐筆轉換成json型態 統整於result_dict
    result_dict = {}
    for _json in json_res_list:
        current_datum = json.loads(_json)
        result_dict = classify_data_by_city(current_datum,result_dict)

    # 將五個城市隨機放進兩個陣列
    result_json = [[],[]]
    
    citys = ['台北市','臺中市','高雄市','新北市','桃園市']
    for i in range(len(citys)):
        random_num = randint(1,2)
        if 1 == random_num:
            result_json[0].append(result_dict[citys[i]])
        else:
            result_json[1].append(result_dict[citys[i]])

    # 將整理完成的data寫入json檔
    for index in range(len(json_file)):
        file_name = json_file[index]
        with open(file_name,'w',encoding='utf-8') as f:
            json_data = result_json[index]
            json.dump(json_data,f,ensure_ascii=False,indent=2)
            f.close()
    spark.stop()
    print ('spark stoped ...')
    print('done')
    return
    
if __name__ == "__main__":
    main()