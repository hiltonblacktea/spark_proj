import sys
sys.path.append('../../spark/')
import findspark
findspark.init()
from pyspark.sql import SparkSession as session
from spark.run_main import get_all_csv , rename_col
spark = session.builder.master('local').appName('test').config('spark.debug.maxToStringFields', 100).getOrCreate()
csv = get_all_csv(spark,'../../')
csv.printSchema()