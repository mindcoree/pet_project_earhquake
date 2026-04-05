import logging
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Конфигурация DAG
OWNER = "mindcore"
DAG_ID = "raw_from_s3_to_stg"  # Изменил на STG

# Используемые таблицы
LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "stg"          # Изменил на STG (staging)
TARGET_TABLE = "earthquake_raw"

# S3 & DB Credentials
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")
PASSWORD = Variable.get("pg_password")

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
    """Fetch data from S3 and store in PostgreSQL Staging layer."""
    
    # Получаем дату выполнения для формирования пути к файлу
    logical_date = context["data_interval_start"].format("YYYY-MM-DD")
    logging.info(f"Processing date: {logical_date}")
    
    con = duckdb.connect()

    try:
        con.sql(f"""
            -- Установка необходимых расширений
            INSTALL httpfs;
            INSTALL postgres;
            LOAD httpfs;
            LOAD postgres;

            -- Конфигурация S3
            SET s3_url_style = 'path';
            SET s3_endpoint = 'minio:9000';
            SET s3_access_key_id = '{ACCESS_KEY}';
            SET s3_secret_access_key = '{SECRET_KEY}';
            SET s3_use_ssl = FALSE;

            -- Подключение к Postgres
            ATTACH 'host=postgres_dwh port=5432 dbname=postgres user=postgres password={PASSWORD}' 
            AS dwh_db (TYPE POSTGRES);

            -- Очистка данных за текущую дату, чтобы избежать дублей при перезапуске
            -- Предполагаем, что в STG есть колонка загрузки или фильтруем по ID
            -- Для простоты здесь просто вставка, но в идеале: 
            -- DELETE FROM dwh_db.{SCHEMA}.{TARGET_TABLE} WHERE date_part = '{logical_date}';

            INSERT INTO dwh_db.{SCHEMA}.{TARGET_TABLE} BY NAME
            SELECT
                time,
                latitude,
                longitude,
                depth,
                mag,
                magType AS mag_type,
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
                depthError AS depth_error,
                magError AS mag_error,
                magNst AS mag_nst,
                status,
                locationSource AS location_source,
                magSource AS mag_source
            FROM read_parquet('s3://prod/{LAYER}/{SOURCE}/{logical_date}/*.parquet');
        """)
        logging.info(f"Data for {logical_date} successfully moved to STG")
    except Exception as e:
        logging.error(f"Error during transfer: {e}")
        raise
    finally:
        con.close()

with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "stg", "pg"],
    catchup=True,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    # Сенсор ждет успешного завершения DAG, который качает данные из API в S3
    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_api_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
    )

    transfer_task = PythonOperator(
        task_id="transfer_s3_to_stg",
        python_callable=get_and_transfer_raw_data_to_stg_pg,
    )

    end = EmptyOperator(task_id="end")

    start >> sensor_on_raw_layer >> transfer_task >> end # type: ignore
    