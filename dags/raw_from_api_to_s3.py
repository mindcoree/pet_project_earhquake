import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

OWNER = "mindcore"
DAG_ID = "raw_from_api_to_s3"

LAYER = "raw"
SOURCE = "earthquake"

LONG_DESCRIPTION = """
This DAG is responsible for fetching data from the earthquake API and storing it in S3.
"""

SHORT_DESCRIPTION = "Fetch data from earthquake API and store in S3."

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 4, 5).subtract(
        days=20
    ),  # start_date = 2026-03-16
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context) -> tuple[str, str]:
    """Get the start and end dates for the API query based on the DAG's execution context."""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date


def get_and_transfer_api_data_to_s3(**context):
    """Fetch data from the earthquake API for the specified date range and store it in S3."""

    start_date, end_date = get_dates(**context)
    logging.info(f"Start load for dates: {start_date}/{end_date}")

    #  Читаем credentials внутри функции — не попадают в логи планировщика
    access_key = Variable.get("access_key")
    secret_key = Variable.get("secret_key")

    con = duckdb.connect()

    try:
        #  Разделяем на отдельные вызовы для удобства дебага
        con.sql("LOAD httpfs;")

        con.sql("SET TIMEZONE = 'UTC';")
        con.sql("SET s3_url_style = 'path';")
        con.sql("SET s3_endpoint = 'minio:9000';")
        con.sql("SET s3_use_ssl = FALSE;")
        con.sql(f"SET s3_access_key_id = '{access_key}';")
        con.sql(f"SET s3_secret_access_key = '{secret_key}';")

        api_url = (
            f"https://earthquake.usgs.gov/fdsnws/event/1/query"
            f"?format=csv&starttime={start_date}&endtime={end_date}"
        )
        s3_path = (
            f"s3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet"
        )

        con.sql(
            f"""
            COPY (
                SELECT * FROM read_csv_auto('{api_url}')
            ) TO '{s3_path}';
        """
        )

        logging.info(f"Download for date success: {start_date}")

    except Exception as e:
        logging.error(f"Error during transfer for {start_date}: {e}")
        raise
    finally:
        con.close()


with DAG(
    dag_id=DAG_ID,
    schedule="0 5 * * *",  #  schedule вместо schedule_interval
    default_args=args,
    tags=["s3", "raw"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    transfer_api_to_s3 = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
    )

    trigger_stg = TriggerDagRunOperator(
        task_id="trigger_stg_layer",
        trigger_dag_id="raw_from_s3_to_stg",
        logical_date="{{ data_interval_start }}",  #  logical_date вместо execution_date
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=30,
    )

    end = EmptyOperator(task_id="end")

    start >> transfer_api_to_s3 >> trigger_stg >> end  # type: ignore
