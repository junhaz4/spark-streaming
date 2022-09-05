from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
userSchema = StructType().add("link", "string").add("rank", "double")

csvDF = spark \
    .readStream \
    .option('delimiter', '\t')\
    .schema(userSchema)\
    .csv("gs://enwiki_w4121/pleaseCheck/block*")
    
df = csvDF.where(csvDF.rank > 0.5)

query = df.writeStream.outputMode("append").format("csv").option("path", "/out").option("checkpointLocation", "/check").start()
print("Waiting")
query.awaitTermination()