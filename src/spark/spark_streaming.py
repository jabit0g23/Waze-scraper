from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, to_timestamp, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import uuid

# === CONFIGURACIÓN DE SPARK Y ESQUEMAS ===
def get_spark_session():
    """Configura y retorna la sesión de Spark."""
    return SparkSession.builder \
        .appName("WazeTrafficProcessor") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.streaming.kafka.maxRatePerPartition", "100") \
        .config("spark.executor.memory", "1g") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

def generate_uuid():
    """Genera un UUID único."""
    return str(uuid.uuid4())

uuid_udf = udf(generate_uuid, StringType())

# Esquemas de Kafka
def get_jams_schema():
    return StructType([
        StructField("idJam", StringType()),
        StructField("commune", StringType()),
        StructField("streetName", StringType()),
        StructField("streetEnd", StringType()),
        StructField("speedKmh", DoubleType()),
        StructField("length", DoubleType()),
        StructField("timestamp", StringType()),
    ])

def get_alerts_schema():
    return StructType([
        StructField("idAlert", StringType()),
        StructField("commune", StringType()),
        StructField("typeAlert", StringType()),
        StructField("streetName", StringType()),
        StructField("timestamp", StringType()),
    ])

# === LECTURA Y PROCESAMIENTO DE KAFKA ===
def read_kafka_stream(spark, topic, schema):
    """Lee un flujo de Kafka y lo parsea según el esquema."""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load() \
        .selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))

# === FUNCIONES DE ESCRITURA ===
# Cassandra
def write_to_cassandra(df, table, keyspace):
    """Escribe un DataFrame en Cassandra."""
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .mode("append") \
        .save()

# Elasticsearch
def write_to_elasticsearch(df, resource, nodes="elasticsearch"):
    """Escribe un DataFrame en Elasticsearch."""
    df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", resource) \
        .option("es.nodes", nodes) \
        .mode("append") \
        .save()

# === STREAMING: JAMS ===
def process_jams(spark):
    """Procesa el flujo de datos de 'jams'."""
    jams_schema = get_jams_schema()
    jams_df = read_kafka_stream(spark, "jams", jams_schema) \
        .withColumnRenamed("idJam", "idjam") \
        .withColumnRenamed("streetName", "streetname") \
        .withColumnRenamed("streetEnd", "streetend") \
        .withColumnRenamed("speedKmh", "speedkmh") \
        .withColumn("idjam", uuid_udf())
        
    # === Eliminar duplicados por 'idjam' y 'timestamp' ===
    jams_df = jams_df.dropDuplicates(["idjam", "timestamp"])
    
    # Filtrado y marca de agua
    jams_filtered = jams_df.filter(col("speedkmh") < 20) \
        .withWatermark("timestamp", "10 minutes")
    
    # Jams_stats
    jams_stats = jams_filtered.groupBy(
        window(col("timestamp"), "5 minutes"),
        col("commune"),
        col("idjam")
    ).count().withColumn("window_start", col("window.start")) \
      .withColumn("window_end", col("window.end")) \
      .drop("window")
    
    # Configuración de streams
    jams_df.writeStream \
        .outputMode("Append") \
        .foreachBatch(lambda df, _: write_to_elasticsearch(df, "jams_raw")) \
        .start()

    jams_filtered.writeStream \
        .outputMode("Append") \
        .foreachBatch(lambda df, _: write_to_cassandra(df, "jams", "waze_traffic")) \
        .start()

    jams_stats.writeStream \
        .outputMode("Update") \
        .foreachBatch(lambda df, _: write_to_elasticsearch(df, "jams_stats_processed")) \
        .start()

# === STREAMING: ALERTS ===
def process_alerts(spark):
    """Procesa el flujo de datos de 'alerts'."""
    alerts_schema = get_alerts_schema()
    alerts_df = read_kafka_stream(spark, "alerts", alerts_schema) \
        .withColumnRenamed("idAlert", "idalert") \
        .withColumnRenamed("typeAlert", "typealert") \
        .withColumnRenamed("streetName", "streetname") \
        .withColumn("idalert", uuid_udf())
        
    # === Eliminar duplicados por 'idalert' y 'timestamp' ===
    alerts_df = alerts_df.dropDuplicates(["idalert", "timestamp"])
    
    # Marca de agua
    alerts_df = alerts_df.withWatermark("timestamp", "10 minutes")
    
    # Alerts_stats
    alerts_stats = alerts_df.groupBy(
        window(col("timestamp"), "5 minutes"),
        col("typealert"),
        col("idalert")
    ).count().withColumn("window_start", col("window.start")) \
      .withColumn("window_end", col("window.end")) \
      .drop("window")
    
    # Configuración de streams
    alerts_df.writeStream \
        .outputMode("Append") \
        .foreachBatch(lambda df, _: write_to_elasticsearch(df, "alerts_raw")) \
        .start()

    alerts_df.writeStream \
        .outputMode("Append") \
        .foreachBatch(lambda df, _: write_to_cassandra(df, "alerts", "waze_traffic")) \
        .start()

    alerts_stats.writeStream \
        .outputMode("Update") \
        .foreachBatch(lambda df, _: write_to_elasticsearch(df, "alerts_stats_processed")) \
        .start()

# === MAIN ===
def main():
    spark = get_spark_session()
    process_jams(spark)
    process_alerts(spark)
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
