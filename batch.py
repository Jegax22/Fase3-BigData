"""
Procesamiento batch de NYC Yellow Taxi Trip Records con PySpark.

Uso:
    python batch.py --input yellow_tripdata_2024-01.parquet --output resultados

El script limpia los datos, crea variables derivadas, calcula agregaciones,
genera gráficos y guarda tablas CSV.
"""
import argparse
from pathlib import Path

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    hour,
    round as spark_round,
    sum as spark_sum,
    when,
)


def main():
    parser = argparse.ArgumentParser(description="Análisis batch de viajes de taxi de NYC")
    parser.add_argument(
        "--input",
        default="yellow_tripdata_2024-01.parquet",
        help="Ruta al archivo Parquet de TLC",
    )
    parser.add_argument(
        "--output",
        default="resultados",
        help="Directorio donde se guardarán CSV y gráficos",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise FileNotFoundError(
            f"No se encontró el archivo: {input_path}. "
            "Descargue el Parquet de TLC y revise --input."
        )

    spark = (
        SparkSession.builder
        .appName("AnalisisNYCTaxiBatch")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    try:
        print(f"Leyendo: {input_path}")
        df = spark.read.parquet(str(input_path))

        print("Cantidad de registros originales:", df.count())
        df.printSchema()

        columnas_requeridas = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
            "total_amount",
            "passenger_count",
            "payment_type",
        ]
        faltantes = [c for c in columnas_requeridas if c not in df.columns]
        if faltantes:
            raise ValueError(f"Faltan columnas requeridas en el Parquet: {faltantes}")

        # Limpieza de valores nulos y registros no válidos.
        df_limpio = df.dropna(
            subset=[
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "trip_distance",
                "total_amount",
                "passenger_count",
            ]
        ).filter(
            (col("trip_distance") > 0)
            & (col("trip_distance") < 200)
            & (col("total_amount") > 0)
            & (col("passenger_count") > 0)
            & (col("passenger_count") <= 6)
            & (col("tpep_dropoff_datetime") >= col("tpep_pickup_datetime"))
        )

        print("Registros después de limpieza:", df_limpio.count())

        # Transformaciones.
        df_transformado = (
            df_limpio
            .withColumn("hora_viaje", hour(col("tpep_pickup_datetime")))
            .withColumn(
                "ingreso_por_milla",
                spark_round(col("total_amount") / col("trip_distance"), 2),
            )
            .withColumn(
                "categoria_distancia",
                when(col("trip_distance") < 3, "Corto")
                .when(col("trip_distance") < 10, "Medio")
                .otherwise("Largo"),
            )
        )

        print("\nEstadísticas descriptivas:")
        df_transformado.select(
            "trip_distance", "passenger_count", "total_amount"
        ).describe().show()

        # Agregaciones.
        viajes_hora = (
            df_transformado.groupBy("hora_viaje")
            .agg(count("*").alias("cantidad_viajes"))
            .orderBy("hora_viaje")
        )

        distancia_promedio = df_transformado.select(
            avg("trip_distance").alias("distancia_promedio")
        )

        ingresos_pago = (
            df_transformado.groupBy("payment_type")
            .agg(
                spark_sum("total_amount").alias("ingresos_totales"),
                count("*").alias("cantidad_viajes"),
            )
            .orderBy(col("ingresos_totales").desc())
        )

        categorias = (
            df_transformado.groupBy("categoria_distancia")
            .agg(count("*").alias("total_viajes"))
            .orderBy("categoria_distancia")
        )

        print("\nViajes por hora:")
        viajes_hora.show(24, truncate=False)
        print("\nDistancia promedio:")
        distancia_promedio.show(truncate=False)
        print("\nIngresos por método de pago:")
        ingresos_pago.show(truncate=False)
        print("\nViajes por categoría de distancia:")
        categorias.show(truncate=False)

        # Guardar tablas. Las carpetas CSV las crea Spark.
        viajes_hora.write.mode("overwrite").option("header", True).csv(
            str(output_dir / "viajes_por_hora")
        )
        ingresos_pago.write.mode("overwrite").option("header", True).csv(
            str(output_dir / "ingresos_por_pago")
        )
        categorias.write.mode("overwrite").option("header", True).csv(
            str(output_dir / "viajes_por_categoria")
        )
        distancia_promedio.write.mode("overwrite").option("header", True).csv(
            str(output_dir / "distancia_promedio")
        )

        # Los resultados agregados son pequeños; por eso se convierten a Pandas.
        pdf_hora = viajes_hora.toPandas()
        plt.figure(figsize=(10, 5))
        plt.bar(pdf_hora["hora_viaje"], pdf_hora["cantidad_viajes"])
        plt.title("Cantidad de viajes por hora")
        plt.xlabel("Hora del día")
        plt.ylabel("Cantidad de viajes")
        plt.xticks(range(24))
        plt.grid(axis="y", alpha=0.3)
        plt.tight_layout()
        plt.savefig(output_dir / "viajes_por_hora.png", dpi=150)
        plt.close()

        pdf_categorias = categorias.toPandas()
        plt.figure(figsize=(8, 5))
        plt.bar(
            pdf_categorias["categoria_distancia"],
            pdf_categorias["total_viajes"],
        )
        plt.title("Distribución de viajes por categoría de distancia")
        plt.xlabel("Categoría de distancia")
        plt.ylabel("Cantidad de viajes")
        plt.tight_layout()
        plt.savefig(output_dir / "viajes_por_categoria.png", dpi=150)
        plt.close()

        print(f"\nProceso terminado. Resultados guardados en: {output_dir.resolve()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
