from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import sys

def main():
    spark = SparkSession.builder \
        .appName("WazeTrafficProcessor") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.streaming.kafka.maxRatePerPartition", "100") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Esquema para los mensajes de 'jams'
    jams_schema = StructType([
        StructField("idJam", StringType()),
        StructField("commune", StringType()),
        StructField("streetName", StringType()),
        StructField("streetEnd", StringType()),
        StructField("speedKmh", DoubleType()),
        StructField("length", DoubleType()),
        StructField("timestamp", StringType()),
    ])

    # Lectura desde el tópico 'jams'
    jams_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "jams") \
        .option("startingOffsets", "latest") \
        .load()

    jams_parsed = jams_df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), jams_schema).alias("data")) \
        .select("data.*")

    jams_parsed = jams_parsed.withColumn("timestamp", to_timestamp(col("timestamp")))

    # Filtrar datos (ejemplo: speedKmh < 20)
    jams_filtered = jams_parsed.filter(col("speedKmh") < 20)

    # Agregar marca de agua
    jams_filtered = jams_filtered.withWatermark("timestamp", "10 minutes")

    # Agrupar por comuna y calcular conteo en ventana de tiempo
    jams_stats = jams_filtered.groupBy(
        window(col("timestamp"), "5 minutes"),
        col("commune")
    ).count()

    # Función para escribir 'jams' en Elasticsearch
    def write_jams_to_elasticsearch(df, epoch_id):
        es_nodes = "http://elasticsearch:9200"
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", "jams_processed") \
            .option("es.nodes", es_nodes) \
            .mode("append") \
            .save()

    jams_query = jams_stats.writeStream \
        .outputMode("Update") \
        .foreachBatch(write_jams_to_elasticsearch) \
        .start()

    # Esquema para los mensajes de 'alerts'
    alerts_schema = StructType([
        StructField("idAlert", StringType()),
        StructField("type", StringType()),
        StructField("subtype", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("timestamp", StringType()),
    ])

    # Lectura desde el tópico 'alerts'
    alerts_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "alerts") \
        .option("startingOffsets", "latest") \
        .load()

    alerts_parsed = alerts_df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), alerts_schema).alias("data")) \
        .select("data.*")

    alerts_parsed = alerts_parsed.withColumn("timestamp", to_timestamp(col("timestamp")))

    # (Opcional) Filtrar o transformar los datos de 'alerts'
    # Por ejemplo, filtrar por tipo de alerta
    # alerts_filtered = alerts_parsed.filter(col("type") == "ACCIDENT")

    # Agregar marca de agua
    alerts_parsed = alerts_parsed.withWatermark("timestamp", "10 minutes")

    # (Opcional) Agrupar por tipo y calcular conteo en ventana de tiempo
    alerts_stats = alerts_parsed.groupBy(
        window(col("timestamp"), "5 minutes"),
        col("type")
    ).count()

    # Función para escribir 'alerts' en Elasticsearch
    def write_alerts_to_elasticsearch(df, epoch_id):
        es_nodes = "http://elasticsearch:9200"
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", "alerts_processed") \
            .option("es.nodes", es_nodes) \
            .mode("append") \
            .save()

    alerts_query = alerts_stats.writeStream \
        .outputMode("Update") \
        .foreachBatch(write_alerts_to_elasticsearch) \
        .start()

    # Espera a que los streamings terminen
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()