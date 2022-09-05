#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import col,udf,split,asc,explode,lower
import regex
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import explode, lower
import sys
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.14.0 pyspark-shell'
spark = SparkSession.builder.getOrCreate()


# In[ ]:


df = spark.read.csv("gs://programming2/p1t2.csv/part-00000-e0927552-f076-4ae6-9b5e-257fb91964bc-c000.csv",sep='\t')



# In[ ]:


df = df.withColumnRenamed("_c0", "title")
df = df.withColumnRenamed("_c1", "link")
df = df.na.drop()

# In[ ]:


import pyspark.sql.functions as f
from pyspark.sql.window import Window

df_new = df.withColumn("rank", f.lit(1))

for _ in range(10):
    df_new = df_new.na.drop()
    df_temp = df_new.withColumn('contribution',f.col('rank')/f.count('title').over(Window.partitionBy('title')))
    df_temp = df_temp.groupBy("link").agg(f.sum("contribution").alias('contribution'))
    df_temp = df_temp.withColumn('rank', 0.85 * f.col('contribution') + 0.15)
    df_temp = df_temp.withColumnRenamed("link", "title")
    df_temp = df_temp.select(["title", "rank"])
    df_new = df.select(["title","link"])
    df_new = df_new.join(df_temp, on="title", how="left")
    df_new = df_new.na.fill(0)
    ret = df_temp.sort(["title","rank"])

gcs_bucket = 'programming2' 
file_path = 'gs://{}/p1t3.csv'.format(gcs_bucket)
ret.limit(5).write.option("delimiter", "\t").csv(path=file_path)

