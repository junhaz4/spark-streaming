from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

from pyspark.sql.functions import lower, col, udf, explode, collect_set, lit
from pyspark.sql.types import ArrayType, StringType

def contribution(row): 
    neighbors = row.neighbors
    rank = row.rank
    num_neighbors = len(neighbors)
    for neighbor in neighbors:
        yield (neighbor, rank / num_neighbors)

df_file = spark.read.option('delimiter', '\t').csv('hdfs:/task2_whole').toDF("link","neighbor")
df_neighbors = df_file.groupBy("link").agg(collect_set('neighbor').alias('neighbors'))
df_ranks = df_neighbors.select("link").withColumn("rank",lit(1.0)) 

for iteration in range(10):    
    #Calculate the contribution for each neighbor
    df_contrib = df_ranks.join(df_neighbors, "link").cache().rdd.flatMap(contribution).toDF(['link','contrib']).groupBy("link").sum("contrib")
    #Update rank
    df_ranks = df_contrib.withColumn("sum(contrib)",col("sum(contrib)")*0.85 + 0.15)
    df_ranks = df_ranks.withColumnRenamed("sum(contrib)","rank")
df_ranks.repartition(1).write.option('delimiter', '\t').csv('hdfs:/pagerank_whole_1')