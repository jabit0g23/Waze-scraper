# Waze Traffic Processor

## Descripción

**Waze Traffic Processor** es una aplicación que procesa datos de tráfico en tiempo real utilizando Apache Spark, Kafka, Cassandra, y Elasticsearch. La aplicación se despliega utilizando Docker y Docker Compose para facilitar la gestión de los diferentes servicios involucrados.

**[Ver video de implementación](https://drive.google.com/file/d/1Oohdhsv3vwmLO8_PtpoxtBbPk_MRh9P5/view?usp=sharing)**

## Prerrequisitos

Antes de comenzar, asegúrate de tener instalados los siguientes componentes en tu sistema:

- [Docker](https://docs.docker.com/get-docker/) (versión 20.10 o superior)
- [Docker Compose](https://docs.docker.com/compose/install/) (versión 1.29 o superior)
- [Node.js](https://nodejs.org/) (versión 14 o superior)
- [Git](https://git-scm.com/) (opcional, para clonar el repositorio)

## Instalación y Ejecución

Sigue estos pasos para configurar y ejecutar la aplicación.

### 1. Clonar el Repositorio (Opcional)

Si aún no tienes el código fuente, clónalo desde el repositorio:

```bash
git clone https://github.com/tu_usuario/waze-traffic-processor.git
cd waze-traffic-processor
```

### 2. Iniciar los Servicios con Docker Compose
Este comando levantará todos los servicios definidos en el archivo docker-compose.yml, incluyendo Spark, Kafka, Cassandra y Elasticsearch.

Abre una terminal y ejecuta:

```bash
docker-compose up --build
```

### 3. Ejecutar la Aplicación Node.js
Abre una nueva terminal y navega al directorio del proyecto si aún no lo has hecho. Luego, ejecuta la aplicación Node.js que probablemente se encarga de generar o manejar los datos de tráfico.


```bash
node ./src/main.js
```

### 4. Configurar Cassandra
Abre otra terminal para interactuar con Cassandra y configurar el keyspace y las tablas necesarias.

a. Acceder a Cassandra con cqlsh
Ejecuta el siguiente comando para acceder al shell de CQL dentro del contenedor de Cassandra:

```bash
docker exec -it cassandra cqlsh
```

b. Ejecutar el Script de Inicialización
Dentro de cqlsh, ejecuta el script de inicialización

```bash
SOURCE '/docker-entrypoint-initdb.d/init.cql';
```

c. Verificar el Keyspace y las Tablas
Aún dentro de cqlsh, selecciona el keyspace waze_traffic y describe las tablas para asegurarte de que todo está

configurado correctamente:

```bash
USE waze_traffic;
DESCRIBE TABLES;
SELECT * FROM jams;
```


### 5. Ejecutar el Script de Spark
Finalmente, abre otra terminal para ejecutar el script de Spark que procesa los datos en tiempo real.

```bash
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0" /app/src/spark/spark_streaming.py

```