import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator



OWNER = "mindcore"
DAG_ID = "stg_to_ods_earthquake"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 4, 5).subtract(
        days=20
    ),  # start_date = 2026-03-16
    "catchup": True,
    "retries": 1,
}

TRANSFORM_SQL = """
INSERT INTO ods.earthquakes (
    id, event_time, updated_at, latitude, longitude, depth,
    mag, mag_type, nst, gap, dmin, rms, net, place,
    region,
    event_type, horizontal_error, depth_error, mag_error,
    mag_nst, status, location_source, mag_source
)
SELECT
    id,
    time::timestamptz,
    updated::timestamptz,
    latitude::numeric,
    longitude::numeric,
    depth::numeric,
    mag::numeric,
    mag_type,
    NULLIF(nst, '')::int,
    NULLIF(gap, '')::numeric,
    NULLIF(dmin, '')::numeric,
    NULLIF(rms, '')::numeric,
    net,
    TRIM(place),
    CASE
        WHEN place ~ ',' THEN TRIM(regexp_replace(place, '^.*,\s*', ''))
        ELSE 'Unknown'
    END,
    lower(type),
    NULLIF(horizontal_error, '')::numeric,
    NULLIF(depth_error, '')::numeric,
    NULLIF(mag_error, '')::numeric,
    NULLIF(mag_nst, '')::int,
    status,
    location_source,
    mag_source
FROM stg.earthquake_raw
ON CONFLICT (id) DO UPDATE SET
    updated_at = EXCLUDED.updated_at,
    mag        = EXCLUDED.mag,
    status     = EXCLUDED.status,
    place      = EXCLUDED.place,
    region     = EXCLUDED.region;
"""

with DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule=None,  #  schedule вместо schedule_interval
    tags=["ods", "pg"],
    max_active_runs=1,
    catchup=True,
) as dag:

    upsert_stg_to_ods = SQLExecuteQueryOperator(
        task_id="upsert_stg_to_ods",
        conn_id="postgres_dwh",
        sql=TRANSFORM_SQL,
    )

    trigger_dm = TriggerDagRunOperator(
    task_id="trigger_dm_layer",
    trigger_dag_id="ods_to_dm_earthquake",
    logical_date="{{ data_interval_start }}",
    reset_dag_run=True,
    wait_for_completion=False,
)

    upsert_stg_to_ods >> trigger_dm  # type: ignore




