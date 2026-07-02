import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

load_dotenv()

OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/opt/airflow/output")
INPUT_PATH = os.path.join(OUTPUT_PATH, "transactions_clean")


def create_spark_session():
    return (
        SparkSession.builder.appName("BankingPipeline_Transform")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_clean_data(spark, path):
    print(f"[transform] Lecture des fichiers Parquet : {path}")
    df = spark.read.parquet(path)
    return df


# KPI 1 — Stats globales
def kpi_global(df):
    result = df.agg(
        F.count("trans_time").alias("nb_transactions"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.max("amount"), 2).alias("max_amount"),
        F.sum("is_fraud").alias("nb_fraud"),
        F.round(F.avg("is_fraud") * 100, 4).alias("fraud_rate_pct"),
    )
    print(f"[transform] kpi_global : {result.count()} ligne")
    return result


# KPI 2 — Par heure
def kpi_by_hour(df):
    result = (
        df.groupBy("trans_hour")
        .agg(
            F.count("trans_time").alias("nb_transactions"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
            F.sum("is_fraud").alias("nb_fraud"),
            F.round(F.avg("is_fraud") * 100, 4).alias("fraud_rate_pct"),
        )
        .orderBy("trans_hour")
    )
    print(f"[transform] kpi_by_hour : {result.count()} lignes")
    return result


# KPI 3 — Par catégorie de montant
def kpi_by_amount_category(df):
    result = (
        df.groupBy("amount_category")
        .agg(
            F.count("trans_time").alias("nb_transactions"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
            F.sum("is_fraud").alias("nb_fraud"),
            F.round(F.avg("is_fraud") * 100, 4).alias("fraud_rate_pct"),
        )
        .orderBy(F.desc("total_amount"))
    )
    print(f"[transform] kpi_by_amount_category : {result.count()} lignes")
    return result


# KPI 4 — Distribution des montants frauduleux vs normaux
def kpi_fraud_vs_normal(df):
    result = df.groupBy("is_fraud").agg(
        F.count("trans_time").alias("nb_transactions"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.min("amount"), 2).alias("min_amount"),
        F.round(F.max("amount"), 2).alias("max_amount"),
    )
    print(f"[transform] kpi_fraud_vs_normal : {result.count()} lignes")
    return result


# KPI 5 — Top 20 montants frauduleux les plus élevés
def kpi_top_fraud_amounts(df):
    result = (
        df.filter(F.col("is_fraud") == 1)
        .select("trans_time", "amount", "amount_category", "trans_hour")
        .orderBy(F.desc("amount"))
        .limit(20)
    )
    print(f"[transform] kpi_top_fraud_amounts : {result.count()} lignes")
    return result


def save_kpi(df, name, output_path):
    path = os.path.join(output_path, "kpis", name)
    df.write.mode("overwrite").parquet(path)
    print(f"[transform] Sauvegardé → {path}")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        df = read_clean_data(spark, INPUT_PATH)

        kpis = {
            "kpi_global": kpi_global(df),
            "kpi_by_hour": kpi_by_hour(df),
            "kpi_by_amount_category": kpi_by_amount_category(df),
            "kpi_fraud_vs_normal": kpi_fraud_vs_normal(df),
            "kpi_top_fraud_amounts": kpi_top_fraud_amounts(df),
        }

        for name, result in kpis.items():
            save_kpi(result, name, OUTPUT_PATH)

        print("\n[transform] Tous les KPIs calculés et sauvegardés avec succès.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
