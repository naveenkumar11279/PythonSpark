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
url ="https://randomuser.me/api/0.8/?results=3"
filename = f"outputfile_{datetime.now().strftime('%Y%m%d')}.csv"
print(filename)

assign_data = spark.read.format("csv").option("header","true").load(r"F:\Big Data\assignment_dataset.csv")
assign_data.show(10)

# data_covert = assign_data.withColumn("timestamp",col("timestamp").cast(TimestampType()))
# data_covert.printSchema()
#
# contents_df = data_covert.withColumn("Ndate",split("timestamp"," ")[0]) \
#                .withColumn("Ntime",split("timestamp"," ")[1]).withColumn("Ntime",split("Ntime",":")[0])
# contents_df.show(10)
assign_data.printSchema()

perfect_data = assign_data.groupBy(window("timestamp","1 hour").alias("hourly_window")) \
    .agg(mean("value").cast(DecimalType(8,2)).alias("avg_v")
         , max("value").alias("max_v")
         , min("value").alias("min_v")
         , stddev("value").cast(DecimalType(8,2)).alias("std_v")) \
    .select(col("hourly_window").start.alias("timestamp"),

"avg_v",
"max_v",
"min_v",
"std_v"
)
perfect_data.printSchema()
perfect_data.show(10)
print(perfect_data.rdd.getNumPartitions())
filepath = f"G:/New folder/{filename}"
# perfect_data.write.format("csv").option("header","true").mode("overwrite").save(filepath)
# perfect_data.write.mode("overwrite").csv(filepath, header=True)
# # perfect_data = contents_df.groupBy(window("timestamp","1 hour").alias("hourly_window")) \
#     .agg(mean("value").alias("avg_v")
#     ,max("value").alias("max_v")
#     ,min("value").alias("min_v")
#     ,stddev("value").alias("std_v")) \
#     .select(col("hourly_window").start.alias("timestamp"),
# "avg_v",
# "max_v",
# "min_v",
# "std_v"
# )
# perfect_data.show(10)

# agg_data1 = contents_df.groupBy("Ndate","Ntime").agg(avg("value").cast(DecimalType(8,2)).alias("average_value")) \
#     .orderBy(col("Ndate").desc(),col("Ntime").desc())
# # agg_data1.show(10)
# agg_data2 = contents_df.groupBy("Ndate","Ntime").agg(max("value").alias("max_value")) \
#     .orderBy(col("Ndate").desc(),col("Ntime").desc())
# # agg_data2.show(10)
#
# agg_data3 = contents_df.groupBy("Ndate","Ntime").agg(min("value").alias("min_value")) \
#     .orderBy(col("Ndate").desc(),col("Ntime").desc())
# # agg_data3.show(10)
#
# agg_data4 = contents_df.groupBy("Ndate","Ntime").agg(stddev("value").cast(DecimalType(8,2)).alias("std_dev_value")) \
#     .orderBy(col("Ndate").desc(),col("Ntime").desc())
# # agg_data3.show(10)
#
# adds_up_data = agg_data1.join(agg_data2, ["Ndate","Ntime"], "inner")\
#     .join(agg_data3,["Ndate","Ntime"],"inner")\
#     .join(agg_data4,["Ndate","Ntime"],"inner")
# adds_up_data.show(10)