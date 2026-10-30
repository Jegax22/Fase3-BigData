"""
Consumidor Kafka con Apache Spark Structured Streaming.

Lee los eventos JSON del topic viajes_taxis y calcula estadísticas acumuladas.
Este ejemplo utiliza una agregación global y outputMode('complete').

Importante:
- Kafka debe estar disponible en localhost:9092.
- El conector Spark-Kafka debe coincidir con la versión de Spark y Scala.
- Ejecute este archivo mediante spark-submit; consulte README.md.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_json, sum as spark_sum
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "viajes_taxis"
CHECKPOINT = "checkpoints/viajes_taxis"


def main():
    spark = (
        SparkSession.builder
        .appName("KafkaSparkStreamingNYCTaxi")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    esquema = StructType(
        [
            StructField("id_viaje", IntegerType(), True),
            StructField("fecha", StringType(), True),
            StructField("pasajeros", IntegerType(), True),
            StructField("distancia", DoubleType(), True),
            StructField("total", DoubleType(), True),
        ]
    )

    try:
        # Lectura continua de mensajes desde Kafka.
        datos_kafka = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "latest")
            .load()
        )

        # Conversión del valor binario de Kafka a JSON estructurado.
        datos = (
            datos_kafka.select(
                from_json(col("value").cast("string"), esquema).alias("evento")
            )
            .select("evento.*")
            .filter(
                col("id_viaje").isNotNull()
                & col("pasajeros").isNotNull()
                & col("distancia").isNotNull()
                & col("total").isNotNull()
            )
        )

        # Agregación global de los eventos recibidos.
        estadisticas = datos.agg(
            count("*").alias("total_viajes"),
            spark_sum("pasajeros").alias("total_pasajeros"),
            avg("distancia").alias("distancia_promedio"),
            spark_sum("total").alias("ingresos_totales"),
        )

        consulta = (
            estadisticas.writeStream
            .outputMode("complete")
            .format("console")
            .option("truncate", False)
            .option("checkpointLocation", CHECKPOINT)
            .start()
        )

        print(f"Spark Structured Streaming está escuchando el topic: {TOPIC}")
        consulta.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
