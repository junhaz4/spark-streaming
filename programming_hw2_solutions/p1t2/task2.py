#!/usr/bin/env python
# coding: utf-8

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.14.0 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col, udf, explode, collect_set
from pyspark.sql.types import ArrayType, StringType

spark = SparkSession.builder.getOrCreate()

import regex as re
# User defined function to extract all links from text

def extract(s):
    all_matches = re.findall(r'\[\[((?:[^[\]]+|(?R))*+)\]\]', s)
    new_matches = []
    for m in all_matches:
        # split by pipe
        n = m.split('|')
        for item in n:
            if not ':' in item or 'Category:' in item:
                if not '#' in item:
                    new_matches.append(item.lower())
                    break
    return new_matches

#Register udf
extract_udf = udf(extract, ArrayType(StringType()))

df = spark.read.format('xml').options(rowTag='page').load('hdfs:/enwiki_test.xml')

#Get the title and text.
df = df.select(lower(col("title")).alias("title"),lower(col("revision.text._VALUE")).alias("text"))
df.show()

#Drop Null values
df = df.na.drop(subset=["text"])

#Extract words and make them into multiple rows
df_flat = df.withColumn('text', extract_udf('text')).withColumn("text",explode("text"))
df_flat.show()


#Write to csv
df_final = df_flat.sort(["title","text"],ascending=True)

results = df_final.head(5)
print(results)

gcs_bucket = 'csee4121_zj' # change it to your bucket name 
gcs_filepath  = 'gs://{}/output/task2_out.csv'.format(gcs_bucket)

print("Begin Writing csv")
df_final.repartition(1).write.save(path=gcs_filepath, format='csv', mode='append', sep='\t')
print("Finish Writing csv")

