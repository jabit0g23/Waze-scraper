from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, to_timestamp, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import uuid

def main():
    spark = SparkSession.builder \
        .appName("WazeTrafficProcessor") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.streaming.kafka.maxRatePerPartition", "100") \
        .config("spark.executor.memory", "1g") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Función para generar UUIDs
    def generate_uuid():
        return str(uuid.uuid4())

    # Registrar la función como UDF
    uuid_udf = udf(generate_uuid, StringType())

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

    # Renombrar 'idJam' a 'idjam' para coincidir con Cassandra
    jams_parsed = jams_parsed.withColumnRenamed("idJam", "idjam")

    # Renombrar las demás columnas a minúsculas
    jams_parsed = jams_parsed \
        .withColumnRenamed("streetName", "streetname") \
        .withColumnRenamed("streetEnd", "streetend") \
        .withColumnRenamed("speedKmh", "speedkmh")

    # Agregar columna 'idjam' si no existe
    # Nota: Dado que ya renombraste 'idJam' a 'idjam', este paso puede ser redundante
    # Si 'idjam' ya existe, no hará nada; de lo contrario, lo agregará
    jams_parsed = jams_parsed.withColumn("idjam", uuid_udf())

    # Filtrar datos (ejemplo: speedkmh < 20)
    jams_filtered = jams_parsed.filter(col("speedkmh") < 20)

    # Agregar marca de agua
    jams_filtered = jams_filtered.withWatermark("timestamp", "10 minutes")

    # Agrupar por comuna e idjam y calcular conteo en ventana de tiempo
    jams_stats = jams_filtered.groupBy(
        window(col("timestamp"), "5 minutes"),
        col("commune"),
        col("idjam")
    ).count()

    # Aplanar la columna 'window' en 'window_start' y 'window_end'
    jams_stats = jams_stats \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .drop("window")

    # Función para escribir 'jams_stats' en Elasticsearch y Cassandra
    def write_jams_stats(df, epoch_id):
        # Depuración: imprimir esquema y algunas filas
        df.printSchema()
        df.show(5)

        es_nodes = "elasticsearch"  # Usar el nombre del servicio de Elasticsearch en Docker Compose
        # Escritura en Elasticsearch
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", "jams_stats_processed") \
            .option("es.nodes", es_nodes) \
            .mode("append") \
            .save()
        
        # Escritura en Cassandra
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="jams_stats", keyspace="waze_traffic") \
            .mode("append") \
            .save()

    jams_stats_query = jams_stats.writeStream \
        .outputMode("Update") \
        .foreachBatch(write_jams_stats) \
        .start()

    # Crear la tabla 'jams' para almacenar datos individuales
    def write_jams_individual(df, epoch_id):
        # Renombrar columnas a minúsculas para coincidir con Cassandra
        df = df \
            .withColumnRenamed("streetName", "streetname") \
            .withColumnRenamed("streetEnd", "streetend") \
            .withColumnRenamed("speedKmh", "speedkmh")

        # Agregar columna 'idjam' si no existe (ya se agregó anteriormente)
        df = df.withColumn("idjam", uuid_udf())

        # Imprimir esquema y algunas filas para depuración
        df.printSchema()
        df.show(5)

        es_nodes = "elasticsearch"  # Usar el nombre del servicio de Elasticsearch en Docker Compose
        # Escritura en Elasticsearch
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", "jams_processed") \
            .option("es.nodes", es_nodes) \
            .mode("append") \
            .save()
        
        # Escritura en Cassandra
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="jams", keyspace="waze_traffic") \
            .mode("append") \
            .save()

    jams_individual_query = jams_filtered.writeStream \
        .outputMode("Append") \
        .foreachBatch(write_jams_individual) \
        .start()

    # Esquema para los mensajes de 'alerts'
    alerts_schema = StructType([
        StructField("idAlert", StringType()),
        StructField("commune", StringType()),
        StructField("typeAlert", StringType()),
        StructField("streetName", StringType()),
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

    # Renombrar 'idAlert' a 'idalert' para coincidir con Cassandra
    alerts_parsed = alerts_parsed.withColumnRenamed("idAlert", "idalert")

    # Renombrar las demás columnas a minúsculas
    alerts_parsed = alerts_parsed \
        .withColumnRenamed("typeAlert", "typealert") \
        .withColumnRenamed("streetName", "streetname")

    # Agregar columna 'idalert' si no existe
    alerts_parsed = alerts_parsed.withColumn("idalert", uuid_udf())

    # Agregar marca de agua
    alerts_parsed = alerts_parsed.withWatermark("timestamp", "10 minutes")

    # Agrupar por tipo de alerta e idalert y calcular conteo en ventana de tiempo
    alerts_stats = alerts_parsed.groupBy(
        window(col("timestamp"), "5 minutes"),
        col("typealert"),
        col("idalert")
    ).count()

    # Aplanar la columna 'window' en 'window_start' y 'window_end'
    alerts_stats = alerts_stats \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .drop("window")

    # Función para escribir 'alerts_stats' en Elasticsearch y Cassandra
    def write_alerts_stats(df, epoch_id):
        # Depuración: imprimir esquema y algunas filas
        df.printSchema()
        df.show(5)

        es_nodes = "elasticsearch"  # Usar el nombre del servicio de Elasticsearch en Docker Compose
        # Escritura en Elasticsearch
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", "alerts_stats_processed") \
            .option("es.nodes", es_nodes) \
            .mode("append") \
            .save()
        
        # Escritura en Cassandra
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="alerts_stats", keyspace="waze_traffic") \
            .mode("append") \
            .save()

    alerts_stats_query = alerts_stats.writeStream \
        .outputMode("Update") \
        .foreachBatch(write_alerts_stats) \
        .start()

    # Crear la tabla 'alerts' para almacenar datos individuales
    def write_alerts_individual(df, epoch_id):
        # Renombrar columnas a minúsculas para coincidir con Cassandra
        df = df \
            .withColumnRenamed("typeAlert", "typealert") \
            .withColumnRenamed("streetName", "streetname")

        # Agregar columna 'idalert' si no existe (ya se agregó anteriormente)
        df = df.withColumn("idalert", uuid_udf())

        # Imprimir esquema y algunas filas para depuración
        df.printSchema()
        df.show(5)

        es_nodes = "elasticsearch"  # Usar el nombre del servicio de Elasticsearch en Docker Compose
        # Escritura en Elasticsearch
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", "alerts_processed") \
            .option("es.nodes", es_nodes) \
            .mode("append") \
            .save()
        
        # Escritura en Cassandra
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="alerts", keyspace="waze_traffic") \
            .mode("append") \
            .save()

    alerts_individual_query = alerts_parsed.writeStream \
        .outputMode("Append") \
        .foreachBatch(write_alerts_individual) \
        .start()

    # Espera a que todos los streamings terminen
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
