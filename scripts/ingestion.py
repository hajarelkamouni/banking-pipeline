import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

load_dotenv()

RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "/opt/airflow/data/raw/fraudTrain.csv")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/opt/airflow/output")


def create_spark_session():
    return (
        SparkSession.builder.appName("BankingPipeline_Ingestion")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_raw_data(spark, path):
    print(f"[ingestion] Lecture du fichier : {path}")
    df = spark.read.csv(path, header=True, inferSchema=True)
    print(f"[ingestion] Colonnes détectées : {df.columns}")
    print(f"[ingestion] Lignes brutes : {df.count():,}")
    return df


def clean_data(df):
    # Supprimer les lignes nulles sur colonnes critiques
    df = df.dropna(subset=["Amount", "Class", "Time"])

    # Supprimer les doublons
    df = df.dropDuplicates()

    # Supprimer les montants négatifs
    df = df.filter(F.col("Amount") > 0)

    # Renommer pour plus de clarté
    df = (
        df.withColumnRenamed("Amount", "amount")
        .withColumnRenamed("Class", "is_fraud")
        .withColumnRenamed("Time", "trans_time")
    )

    # Ajouter des tranches horaires (Time est en secondes)
    df = df.withColumn(
        "trans_hour", (F.col("trans_time") / 3600 % 24).cast(IntegerType())
    )

    # Catégoriser le montant
    df = df.withColumn(
        "amount_category",
        F.when(F.col("amount") < 10, "micro")
        .when(F.col("amount") < 100, "small")
        .when(F.col("amount") < 1000, "medium")
        .otherwise("large"),
    )

    print(f"[ingestion] Lignes après nettoyage : {df.count():,}")
    return df


def save_as_parquet(df, output_path):
    path = os.path.join(output_path, "transactions_clean")
    print(f"[ingestion] Sauvegarde Parquet dans : {path}")
    df.write.mode("overwrite").parquet(path)
    print("[ingestion] Sauvegarde terminée.")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        df_raw = read_raw_data(spark, RAW_DATA_PATH)
        df_clean = clean_data(df_raw)
        save_as_parquet(df_clean, OUTPUT_PATH)
        print("[ingestion] Pipeline d'ingestion terminé avec succès.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
