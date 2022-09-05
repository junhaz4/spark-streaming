from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "PageRankEmitter")
ssc = StreamingContext(sc, 1)
ds = ssc.textFileStream("gs://enwiki_w4121/pagerank_whole")
ds.saveAsTextFiles("gs://enwiki_w4121/pleaseCheck/blocks")
ssc.start()
ssc.awaitTermination()