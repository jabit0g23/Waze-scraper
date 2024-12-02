from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, to_timestamp, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import uuid

def main():
    # Configuración del entorno de Spark
    spark = SparkSession.builder \
        .appName("WazeTrafficProcessor") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.streaming.kafka.maxRatePerPartition", "100") \
        .config("spark.executor.memory", "1g") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

    # Establecer nivel de log de Spark
    spark.sparkContext.setLogLevel("WARN")

    # Generar un UUID único para cada registro
    def generate_uuid():
        return str(uuid.uuid4())

    # Registrar la función de generación de UUID como una UDF (User Defined Function)
    uuid_udf = udf(generate_uuid, StringType())


    # === PROCESAMIENTO DE JAMS Y ALERTS===
    
    # Definir el esquema para los mensajes de 'jams' y 'alerts'
    jams_schema = StructType([
        StructField("idJam", StringType()),
        StructField("commune", StringType()),
        StructField("streetName", StringType()),
        StructField("streetEnd", StringType()),
        StructField("speedKmh", DoubleType()),
        StructField("length", DoubleType()),
        StructField("timestamp", StringType()),
    ])
    
    alerts_schema = StructType([
        StructField("idAlert", StringType()),
        StructField("commune", StringType()),
        StructField("typeAlert", StringType()),
        StructField("streetName", StringType()),
        StructField("timestamp", StringType()),
    ])

    # Leer mensajes del tópico 'jams' y 'alerts' en Kafka
    jams_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "jams") \
        .option("startingOffsets", "latest") \
        .load()
        
    alerts_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "alerts") \
        .option("startingOffsets", "latest") \
        .load()

    # Parsear el contenido JSON de los mensajes
    jams_parsed = jams_df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), jams_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))  # Convertir a formato de tiempo
        
    alerts_parsed = alerts_df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), alerts_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))  # Convertir a formato de tiempo
        
    # Renombrar columnas para ajustarse al formato de Cassandra
    jams_parsed = jams_parsed \
        .withColumnRenamed("idJam", "idjam") \
        .withColumnRenamed("streetName", "streetname") \
        .withColumnRenamed("streetEnd", "streetend") \
        .withColumnRenamed("speedKmh", "speedkmh")
        
    alerts_parsed = alerts_parsed \
        .withColumnRenamed("idAlert", "idalert") \
        .withColumnRenamed("typeAlert", "typealert") \
        .withColumnRenamed("streetName", "streetname")

    # ==================
    # === Jams_stats ===
    # ==================
    
    jams_parsed = jams_parsed.withColumn("idjam", uuid_udf()) # Agregar una columna de UUID si no existe
    jams_filtered = jams_parsed.filter(col("speedkmh") < 20) # Filtrar datos para análisis (por ejemplo: atascos con velocidad < 20 km/h)
    jams_filtered = jams_filtered.withWatermark("timestamp", "10 minutes") # Marca de agua para ventanas de tiempo en datos en streaming

    # Crear estadísticas (jams_stats) agrupadas por comuna y ventana de tiempo
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
    
    # ==================
    # == alerts_stats ==
    # ==================
    
    alerts_parsed = alerts_parsed.withColumn("idalert", uuid_udf())  # Agregar una columna de UUID si no existe
    alerts_parsed = alerts_parsed.withWatermark("timestamp", "10 minutes") # Marca de agua para ventanas de tiempo en datos en streaming

    # Crear estadísticas agrupadas por tipo de alerta y ventana de tiempo
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



    # Función para escribir jams_stats en Elasticsearch y Cassandra
    def write_jams_stats(df, epoch_id):
        df.printSchema()  # Mostrar esquema de depuración
        df.show(5)  # Mostrar algunas filas para validación

        es_nodes = "elasticsearch"
        # Escribir en Elasticsearch
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", "jams_stats_processed") \
            .option("es.nodes", es_nodes) \
            .mode("append") \
            .save()

        # Escribir en Cassandra
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="jams_stats", keyspace="waze_traffic") \
            .mode("append") \
            .save()

    # Función para escribir jams individuales en Elasticsearch y Cassandra
    def write_jams_individual(df, epoch_id):
        df.printSchema()  # Mostrar esquema de depuración
        df.show(5)  # Mostrar algunas filas para validación

        es_nodes = "elasticsearch"
        # Escribir en Elasticsearch
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", "jams_processed") \
            .option("es.nodes", es_nodes) \
            .mode("append") \
            .save()

        # Escribir en Cassandra
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="jams", keyspace="waze_traffic") \
            .mode("append") \
            .save()

    # Configuración de stream para estadísticas y datos individuales de jams
    jams_stats_query = jams_stats.writeStream \
        .outputMode("Update") \
        .foreachBatch(write_jams_stats) \
        .start()

    jams_individual_query = jams_filtered.writeStream \
        .outputMode("Append") \
        .foreachBatch(write_jams_individual) \
        .start()


    # Función para escribir alerts_stats en Elasticsearch y Cassandra
    def write_alerts_stats(df, epoch_id):
        df.printSchema()  # Mostrar esquema de depuración
        df.show(5)  # Mostrar algunas filas para validación

        es_nodes = "elasticsearch"
        # Escribir en Elasticsearch
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", "alerts_stats_processed") \
            .option("es.nodes", es_nodes) \
            .mode("append") \
            .save()

        # Escribir en Cassandra
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="alerts_stats", keyspace="waze_traffic") \
            .mode("append") \
            .save()

    # Configuración de stream para estadísticas de alertas
    alerts_stats_query = alerts_stats.writeStream \
        .outputMode("Update") \
        .foreachBatch(write_alerts_stats) \
        .start()

    # Esperar finalización de streams
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
