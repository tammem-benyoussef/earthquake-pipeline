from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

PROJECT_ROOT = Variable.get("PROJECT_ROOT", default_var="/home/msi/earthquake-pipeline")
DBT_TARGET = Variable.get("DBT_TARGET", default_var="dev")

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="earthquake_daily",
    schedule="0 3 * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    default_args=default_args,
    tags=["earthquake-pipeline"],
) as dag:

    ingest = BashOperator(
        task_id="ingest",
        bash_command=(
            f"docker run --rm --network host --env-file {PROJECT_ROOT}/.env "
            "earthquake-ingestion --start-date {{ ds }} --end-date {{ ds }}"
        ),
    )

    spark_process = BashOperator(
        task_id="spark_process",
        bash_command=(
            f"docker run --rm --network host --env-file {PROJECT_ROOT}/.env "
            "earthquake-spark --start-date {{ ds }} --end-date {{ ds }}"
        ),
    )

    dbt_build = BashOperator(
    task_id="dbt_build",
    bash_command=(
        f"docker run --rm --network host --env-file {PROJECT_ROOT}/.env "
        f"-v /home/msi/.dbt:/root/.dbt "
        f"dbt-earthquakes build --target {DBT_TARGET}"
    ),
)

    ingest >> spark_process >> dbt_build