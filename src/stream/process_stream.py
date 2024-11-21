from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder.appName("TrafficProcessor").getOrCreate()

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic_jams,traffic_alerts") \
    .load()

schema = StructType() \
    .add("id", StringType()) \
    .add("commune", StringType()) \
    .add("streetName", StringType()) \
    .add("speedKmh", IntegerType()) \
    .add("length", IntegerType()) \
    .add("timestamp", StringType())

traffic_data = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")
traffic_data = traffic_data.filter("speedKmh < 20")

query = traffic_data.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
