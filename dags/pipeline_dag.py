from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# ─────────────────────────────────────────────
# Arguments par défaut du DAG
# ─────────────────────────────────────────────
default_args = {
    "owner": "hajar",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ─────────────────────────────────────────────
# Définition du DAG
# ─────────────────────────────────────────────
with DAG(
    dag_id="Fraud_pipeline",
    description="Pipeline ETL bancaire",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["banking", "etl", "pyspark"],
) as dag:
    # ─────────────────────────────────────────
    # Variables d'environnement partagées
    # ─────────────────────────────────────────
    env_vars = {
        "POSTGRES_HOST": "postgres",
        "POSTGRES_PORT": "5432",
        "BANKING_DB": "banking",
        "BANKING_USER": "airflow",
        "BANKING_PASSWORD": "airflow",
        "RAW_DATA_PATH": "/opt/airflow/data/raw/fraudTrain.csv",
        "OUTPUT_PATH": "/opt/airflow/output",
        "PYTHONPATH": "/opt/airflow/scripts",
    }

    # ─────────────────────────────────────────
    # Définition des tâches
    # ─────────────────────────────────────────
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    task_check_source = BashOperator(
        task_id="check_source_file",
        bash_command="""
            set -euo pipefail

            if [ ! -f "$RAW_DATA_PATH" ]; then
                echo "ERREUR : Fichier source introuvable : $RAW_DATA_PATH"
                exit 1
            fi

            SIZE=$(du -sh "$RAW_DATA_PATH" | cut -f1)
            echo "Fichier source trouvé : $RAW_DATA_PATH ($SIZE)"
        """,
        env=env_vars,
        doc_md="Vérifie que fraudTrain.csv est présent dans data/raw/",
    )

    task_install_deps = BashOperator(
        task_id="install_dependencies",
        bash_command="""
            set -euo pipefail

            DEPS=(
                "pyspark"
                "pandas"
                "pyarrow"
                "psycopg2-binary"
                "python-dotenv"
            )

            python -m pip install --quiet "${DEPS[@]}"
            echo "Dépendances installées avec succès."
        """,
        execution_timeout=timedelta(minutes=10),
        doc_md="Installe les dépendances Python nécessaires.",
    )

    task_ingestion = BashOperator(
        task_id="ingestion",
        bash_command="python /opt/airflow/scripts/ingestion.py",
        env=env_vars,
        doc_md="Lit le CSV brut, nettoie les données et sauvegarde en Parquet.",
        execution_timeout=timedelta(minutes=30),
    )

    task_check_parquet = BashOperator(
        task_id="check_parquet_output",
        bash_command="""
            set -euo pipefail

            if [ ! -d "$OUTPUT_PATH/transactions_clean" ]; then
                echo "ERREUR : Dossier Parquet introuvable."
                exit 1
            fi

            COUNT=$(
                find "$OUTPUT_PATH/transactions_clean" \
                    -type f \
                    -name "*.parquet" \
                | wc -l
            )

            echo "$COUNT fichiers Parquet trouvés."
        """,
        env=env_vars,
        doc_md="Vérifie que les fichiers Parquet ont été produits.",
    )

    task_transform = BashOperator(
        task_id="transformation",
        bash_command="python /opt/airflow/scripts/transform.py",
        env=env_vars,
        doc_md="Calcule les KPIs bancaires depuis les fichiers Parquet.",
        execution_timeout=timedelta(minutes=30),
    )

    task_check_kpis = BashOperator(
        task_id="check_kpis_output",
        bash_command="""
            set -euo pipefail

            KPI_LIST=(
                "kpi_global"
                "kpi_by_hour"
                "kpi_by_amount_category"
                "kpi_fraud_vs_normal"
                "kpi_top_fraud_amounts"
            )

            for KPI in "${KPI_LIST[@]}"; do
                if [ ! -d "$OUTPUT_PATH/kpis/$KPI" ]; then
                    echo "ERREUR : KPI manquant : $KPI"
                    exit 1
                fi
            done

            echo "Tous les KPIs sont présents."
        """,
        env=env_vars,
        doc_md="Vérifie que tous les KPIs Parquet sont présents.",
    )

    task_load = BashOperator(
        task_id="load_to_postgres",
        bash_command="python /opt/airflow/scripts/load.py",
        env=env_vars,
        doc_md="Charge les KPIs dans PostgreSQL.",
        execution_timeout=timedelta(minutes=15),
    )

    # ─────────────────────────────────────────
    # Ordre d'exécution des tâches
    # ─────────────────────────────────────────
    (
        start
        >> task_check_source
        >> task_install_deps
        >> task_ingestion
        >> task_check_parquet
        >> task_transform
        >> task_check_kpis
        >> task_load
        >> end
    )
