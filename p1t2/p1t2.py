#!/usr/bin/env python
# coding: utf-8


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='page').load('hdfs:/enwiki_whole.xml')


import regex
from pyspark.sql.types import IntegerType, StringType, ArrayType
from pyspark.sql.functions import col,udf,split,asc,explode,lower



def find_match(row):
    result = []
    if row is None:
        return result
    pattern = r'\[\[((?:[^[\]]+|(?R))*+)\]\]'
    string = str(row).split("text=Row(_VALUE=")[-1]
    matchs = regex.findall(pattern,str(string))
    for match in matchs:
        m = match.split('|')
        for i in m:
            if '#' in i:
                continue
            elif ':' in i and 'Category' not in i:
                continue
            elif i=="":
                continue
            else:
                result.append(i.lower())
                break
    return result 



find_link_udf = udf(lambda row: find_match(str(row)), ArrayType(StringType()))
df_test = df.select("title",find_link_udf("revision").alias("match"))
df2 = df_test.withColumn('title',lower("title")).withColumn('link',explode('match')).select(['title','link'])


gcs_bucket = 'programming2' 
file_path = 'gs://{}/p1t2.csv'.format(gcs_bucket)
df2.sort(asc('title'),asc('link')).limit(5).write.csv(path=file_path,sep="\t")





