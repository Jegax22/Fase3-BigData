# Proyecto Big Data: análisis de viajes NYC Yellow Taxi

Este repositorio contiene los scripts para el procesamiento batch con PySpark y la simulación de procesamiento en tiempo real con Apache Kafka y Spark Structured Streaming.

## Estructura

- `batch.py`: carga el archivo Parquet, limpia y transforma los datos, calcula agregaciones y genera gráficos.
- `producer.py`: genera eventos JSON simulados y los publica en el topic `viajes_taxis`.
- `consumer.py`: consume eventos desde Kafka y calcula estadísticas con Spark Structured Streaming.
- `docker-compose.yml`: configuración local de un broker Kafka.
- `requirements.txt`: dependencias Python.

## Requisitos

- Python 3.10 o 3.11 recomendado.
- Java compatible con la versión de Apache Spark instalada.
- Docker Desktop o Docker Engine para Kafka.
- Apache Spark / PySpark 3.5.6 para los comandos de este ejemplo.

Instale las dependencias:

```bash
python -m pip install -r requirements.txt
```

## 1. Descargar el conjunto de datos

Descargue el archivo `yellow_tripdata_2024-01.parquet` desde la página oficial de TLC:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Guárdelo en la carpeta del proyecto o indique su ruta mediante `--input`.

## 2. Ejecutar el procesamiento batch

```bash
python batch.py --input yellow_tripdata_2024-01.parquet --output resultados
```

El programa imprime estadísticas y guarda archivos CSV (en directorios) y gráficos PNG dentro de `resultados/`.

## 3. Iniciar Kafka

Desde la carpeta que contiene `docker-compose.yml`:

```bash
docker compose up -d
```

Crear el topic:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic viajes_taxis \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

Si el topic ya existe, Kafka indicará que no puede crearlo de nuevo; en ese caso puede continuar.

## 4. Ejecutar el productor

En una terminal:

```bash
python producer.py
```

El productor envía un evento simulado cada dos segundos. Deténgalo con `Ctrl + C`.

## 5. Ejecutar Spark Structured Streaming

En otra terminal, desde la carpeta del proyecto, ejecute:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  consumer.py
```

El artefacto `_2.12:3.5.6` corresponde a Spark 3.5.6 con Scala 2.12. Si su instalación utiliza otra versión de Spark o Scala, debe sustituir el conector por la combinación compatible.

El consumidor muestra en consola el número acumulado de viajes, pasajeros, distancia promedio e ingresos de los eventos recibidos.

## Orden de ejecución resumido

1. Iniciar Kafka con Docker.
2. Crear el topic `viajes_taxis`.
3. Ejecutar `consumer.py` con `spark-submit`.
4. Ejecutar `producer.py` en otra terminal.
5. Observar cómo Spark actualiza las estadísticas.
6. Detener los procesos y conservar capturas de evidencia.

## Consideraciones para el informe

- El archivo histórico TLC se utiliza en el análisis batch.
- Los eventos del productor son simulados; no representan viajes reales en directo.
- Incluya en el informe únicamente resultados obtenidos al ejecutar los programas.
- Añada capturas de la ejecución, gráficos y tablas reales.
- Compruebe las versiones de Python, Java, Spark y Scala en el equipo donde se ejecute el proyecto.

## Fuente del conjunto de datos

New York City Taxi and Limousine Commission (TLC):  
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
