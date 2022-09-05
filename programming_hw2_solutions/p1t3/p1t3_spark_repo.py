# Solution adapted from Spark official github repository
# Ref: https://github.com/apache/spark/blob/master/examples/src/main/python/pagerank.py

from pyspark.sql import SparkSession
from operator import add

spark = SparkSession.builder.getOrCreate()

df = spark.read.format("csv").option("delimiter", "\t").load('hdfs:/task2_whole')
df = df.withColumnRenamed("_c0","from").withColumnRenamed("_c1", "to")

graph = df.rdd.distinct()
graph = graph.groupByKey()

ranks = graph.map(lambda neighbors: (neighbors[0], 1.0))

def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


for iteration in range(10):
    #join graph and ranks on 'from' and compute the contributions
    contribs = graph.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
        url_urls_rank[1][0], url_urls_rank[1][1]  # type: ignore[arg-type]
    ))
    
    # add all contribs and update their ranks
    ranks = contribs.reduceByKey(add).mapValues(lambda c: c * 0.85 + 0.15)

cols = ["article", "rank"]
df_ranks = ranks.toDF(cols)

df_ranks.repartition(1).write.option('delimiter', '\t').csv('hdfs:/pagerank_whole_1')