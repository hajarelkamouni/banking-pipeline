import os

import psycopg2
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("BANKING_DB", "banking"),
    "user": os.getenv("BANKING_USER", "airflow"),
    "password": os.getenv("BANKING_PASSWORD", "airflow"),
}

OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/opt/airflow/output")
KPIS_PATH = os.path.join(OUTPUT_PATH, "kpis")


def get_connection():
    print("[load] Connexion à PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def create_tables(conn):
    queries = [
        """
        CREATE TABLE IF NOT EXISTS kpi_global (
            nb_transactions  INTEGER,
            total_amount     FLOAT,
            avg_amount       FLOAT,
            max_amount       FLOAT,
            nb_fraud         INTEGER,
            fraud_rate_pct   FLOAT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS kpi_by_hour (
            trans_hour       INTEGER PRIMARY KEY,
            nb_transactions  INTEGER,
            total_amount     FLOAT,
            avg_amount       FLOAT,
            nb_fraud         INTEGER,
            fraud_rate_pct   FLOAT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS kpi_by_amount_category (
            amount_category  VARCHAR(20) PRIMARY KEY,
            nb_transactions  INTEGER,
            total_amount     FLOAT,
            avg_amount       FLOAT,
            nb_fraud         INTEGER,
            fraud_rate_pct   FLOAT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS kpi_fraud_vs_normal (
            is_fraud         INTEGER PRIMARY KEY,
            nb_transactions  INTEGER,
            total_amount     FLOAT,
            avg_amount       FLOAT,
            min_amount       FLOAT,
            max_amount       FLOAT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS kpi_top_fraud_amounts (
            trans_time       FLOAT,
            amount           FLOAT,
            amount_category  VARCHAR(20),
            trans_hour       INTEGER
        );
        """,
    ]
    with conn.cursor() as cur:
        for query in queries:
            cur.execute(query)
    conn.commit()
    print("[load] Tables créées.")


def read_parquet(name):
    path = os.path.join(KPIS_PATH, name)
    print(f"[load] Lecture : {path}")
    df = pq.read_table(path).to_pandas()
    print(f"[load] {name} → {len(df):,} lignes")
    return df


def load_table(conn, df, table, conflict_col=None):
    if df.empty:
        print(f"[load] {table} : vide.")
        return
    cols = list(df.columns)
    cols_str = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))

    if conflict_col:
        update_cols = [c for c in cols if c != conflict_col]
        update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
        query = f"""
            INSERT INTO {table} ({cols_str})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_col}) DO UPDATE SET {update_str};
        """
    else:
        query = f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders});"

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    with conn.cursor() as cur:
        if not conflict_col:
            cur.execute(f"TRUNCATE TABLE {table};")
        cur.executemany(query, rows)
    conn.commit()
    print(f"[load] {table} → {len(rows):,} lignes chargées.")


def main():
    conn = get_connection()
    try:
        create_tables(conn)

        load_table(conn, read_parquet("kpi_global"), "kpi_global")
        load_table(conn, read_parquet("kpi_by_hour"), "kpi_by_hour", "trans_hour")
        load_table(
            conn,
            read_parquet("kpi_by_amount_category"),
            "kpi_by_amount_category",
            "amount_category",
        )
        load_table(
            conn, read_parquet("kpi_fraud_vs_normal"), "kpi_fraud_vs_normal", "is_fraud"
        )
        load_table(conn, read_parquet("kpi_top_fraud_amounts"), "kpi_top_fraud_amounts")

        print("\n[load] Tous les KPIs chargés dans PostgreSQL avec succès.")
    except Exception as e:
        conn.rollback()
        print(f"[load] ERREUR : {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
