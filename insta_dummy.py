from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from datetime import datetime
from functools import reduce
import json
import requests
from pyspark.sql.types import *
# Words count program in pyspark
conf = SparkConf().setAppName("Practice").setMaster("local[*]")
sc = SparkContext(conf=conf)
spark = SparkSession.builder\
    .appName("Myapp")\
    .master("local[*]")\
         .getOrCreate()
print("Application stating from here==>")

Insta_raw_df = spark.read.format("csv").option("header","true").load(r'D:\Data\insta_dummy.csv')
Insta_raw_df.show(truncate=False)

currentdate = "2024-11-04 08:25:00"

timestampde = Insta_raw_df.withColumn("Timestamp", to_timestamp(lit(col("Timestamp"))))
ladata = timestampde.filter(
    col("Timestamp") > date_sub(to_date(lit(currentdate)),1)
)

timestampde.show()
print(currentdate)
print(timestampde.printSchema())
print("updating")
print("updating2")

