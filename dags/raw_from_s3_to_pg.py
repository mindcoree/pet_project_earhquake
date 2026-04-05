import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

OWNER = "mindcore"
DAG_ID = "raw_from_s3_to_stg"

LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "stg"
TARGET_TABLE = "earthquake_raw"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 4, 5).subtract(
        days=20
    ),  # start_date = 2026-03-16
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=10),
}


def get_and_transfer_raw_data_to_stg_pg(**context):
    """Fetch data from S3 and upsert into PostgreSQL Staging layer."""

    logical_date = context["data_interval_start"].format("YYYY-MM-DD")
    logging.info(f"Processing date: {logical_date}")

    #  Читаем credentials внутри функции
    access_key = Variable.get("access_key")
    secret_key = Variable.get("secret_key")
    password = Variable.get("pg_password")

    con = duckdb.connect()

    try:
        #  LOAD вместо INSTALL — расширения уже установлены в контейнере,
        #    INSTALL каждый раз лезет в сеть и тормозит выполнение
        con.sql("LOAD httpfs;")
        con.sql("LOAD postgres;")

        con.sql("SET s3_url_style = 'path';")
        con.sql("SET s3_endpoint = 'minio:9000';")
        con.sql("SET s3_use_ssl = FALSE;")
        con.sql(f"SET s3_access_key_id = '{access_key}';")
        con.sql(f"SET s3_secret_access_key = '{secret_key}';")

        pg_conn_str = (
            f"host=postgres_dwh port=5432 dbname=postgres "
            f"user=postgres password={password}"
        )
        con.sql(f"ATTACH '{pg_conn_str}' AS dwh_db (TYPE POSTGRES);")

        parquet_path = f"s3://prod/{LAYER}/{SOURCE}/{logical_date}/*.parquet"
        source_data = con.sql(
            f"""
            SELECT
                time,
                latitude,
                longitude,
                depth,
                mag,
                magType         AS mag_type,
                nst,
                gap,
                dmin,
                rms,
                net,
                id,
                updated,
                place,
                type,
                horizontalError AS horizontal_error,
                depthError      AS depth_error,
                magError        AS mag_error,
                magNst          AS mag_nst,
                status,
                locationSource  AS location_source,
                magSource       AS mag_source
            FROM read_parquet('{parquet_path}')
        """
        )

        row_count = source_data.count("id").fetchone()[0]  # type: ignore
        logging.info(f"Rows read from S3: {row_count}")

        if row_count == 0:
            logging.warning(f"No data found in S3 for {logical_date}, skipping insert.")
            return

        #  DELETE + INSERT — идемпотентный upsert
        #    ON CONFLICT не поддерживается DuckDB postgres extension
        logging.info(f"Deleting existing rows for {logical_date}...")
        con.sql(
            f"""
            DELETE FROM dwh_db.{SCHEMA}.{TARGET_TABLE}
            WHERE CAST(time AS DATE) = '{logical_date}'
        """
        )

        logging.info("Inserting fresh data...")
        con.sql(
            f"""
            INSERT INTO dwh_db.{SCHEMA}.{TARGET_TABLE} BY NAME
            SELECT * FROM source_data
        """
        )

        logging.info(f"Successfully upserted {row_count} rows for {logical_date}.")

    except Exception as e:
        logging.error(f"Error during transfer for {logical_date}: {e}")
        raise
    finally:
        con.close()


with DAG(
    dag_id=DAG_ID,
    schedule=None,  #  schedule вместо schedule_interval
    default_args=args,
    tags=["s3", "stg", "pg"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    transfer_task = PythonOperator(
        task_id="transfer_s3_to_stg",
        python_callable=get_and_transfer_raw_data_to_stg_pg,
    )

    trigger_ods = TriggerDagRunOperator(
        task_id="trigger_ods_layer",
        trigger_dag_id="stg_to_ods_earthquake",
        logical_date="{{ data_interval_start }}",  #  logical_date вместо execution_date
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=30,
    )

    end = EmptyOperator(task_id="end")

    start >> transfer_task >> trigger_ods >> end  # type: ignore
