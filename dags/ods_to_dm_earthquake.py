import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

OWNER = "mindcore"
DAG_ID = "ods_to_dm_earthquake"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 4, 5).subtract(days=20),  # start_date = 2026-03-16
    "catchup": True,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}

# ── Dimensions ────────────────────────────────────────────────────────────────

SQL_DIM_REGION = """
INSERT INTO dm.dim_region (region_name)
SELECT DISTINCT COALESCE(NULLIF(TRIM(region), ''), 'unknown') AS region_name
FROM ods.earthquakes
WHERE CAST(event_time AS DATE) = '{{ ds }}'
ON CONFLICT (region_name) DO NOTHING;
"""

SQL_DIM_TIME = """
INSERT INTO dm.dim_time (
    event_time, year, month, day, hour, quarter, day_of_week, is_weekend
)
SELECT DISTINCT
    event_time,
    EXTRACT(YEAR    FROM event_time)::INT,
    EXTRACT(MONTH   FROM event_time)::INT,
    EXTRACT(DAY     FROM event_time)::INT,
    EXTRACT(HOUR    FROM event_time)::INT,
    EXTRACT(QUARTER FROM event_time)::INT,
    TO_CHAR(event_time, 'Day'),
    EXTRACT(DOW FROM event_time) IN (0, 6)
FROM ods.earthquakes
WHERE CAST(event_time AS DATE) = '{{ ds }}'
ON CONFLICT (event_time) DO NOTHING;
"""

SQL_DIM_LOCATION = """
INSERT INTO dm.dim_location (region_id, latitude, longitude, place)
SELECT DISTINCT
    r.region_id,
    e.latitude,
    e.longitude,
    COALESCE(NULLIF(TRIM(e.place), ''), 'unknown') AS place
FROM ods.earthquakes e
LEFT JOIN dm.dim_region r
       ON r.region_name = COALESCE(NULLIF(TRIM(e.region), ''), 'unknown')
WHERE CAST(e.event_time AS DATE) = '{{ ds }}'
ON CONFLICT (latitude, longitude, place) DO NOTHING;
"""

SQL_DIM_MAGNITUDE = """
INSERT INTO dm.dim_magnitude (mag_type, mag_source)
SELECT DISTINCT
    COALESCE(NULLIF(TRIM(e.mag_type), ''), 'unknown') AS mag_type,
    COALESCE(NULLIF(TRIM(e.mag_source), ''), 'unknown') AS mag_source
FROM ods.earthquakes e
WHERE CAST(e.event_time AS DATE) = '{{ ds }}'
ON CONFLICT (mag_type, mag_source) DO NOTHING;
"""

SQL_DIM_NETWORK = """
INSERT INTO dm.dim_network (net, location_source)
SELECT DISTINCT
    COALESCE(NULLIF(TRIM(e.net), ''), 'unknown') AS net,
    COALESCE(NULLIF(TRIM(e.location_source), ''), 'unknown') AS location_source
FROM ods.earthquakes e
WHERE CAST(e.event_time AS DATE) = '{{ ds }}'
ON CONFLICT (net, location_source) DO NOTHING;
"""

SQL_DIM_STATUS = """
INSERT INTO dm.dim_status (status)
SELECT DISTINCT COALESCE(NULLIF(TRIM(status), ''), 'unknown') AS status
FROM ods.earthquakes
WHERE CAST(event_time AS DATE) = '{{ ds }}'
ON CONFLICT (status) DO NOTHING;
"""

SQL_DIM_EVENT_TYPE = """
INSERT INTO dm.dim_event_type (event_type)
SELECT DISTINCT COALESCE(NULLIF(TRIM(event_type), ''), 'unknown') AS event_type
FROM ods.earthquakes
WHERE CAST(event_time AS DATE) = '{{ ds }}'
ON CONFLICT (event_type) DO NOTHING;
"""

# ── Fact table ────────────────────────────────────────────────────────────────

SQL_FACT = """
INSERT INTO dm.fact_earthquakes (
    id,
    time_id,
    location_id,
    mag_type_id,
    network_id,
    status_id,
    event_type_id,
    mag,
    depth,
    nst,
    gap,
    dmin,
    rms,
    horizontal_error,
    depth_error,
    mag_error,
    mag_nst
)
SELECT
    e.id,
    t.time_id,
    l.location_id,
    m.mag_type_id,
    n.network_id,
    s.status_id,
    et.event_type_id,
    e.mag,
    e.depth,
    e.nst,
    e.gap,
    e.dmin,
    e.rms,
    e.horizontal_error,
    e.depth_error,
    e.mag_error,
    e.mag_nst
FROM ods.earthquakes e
LEFT JOIN dm.dim_time         t  ON t.event_time                                    = e.event_time
LEFT JOIN dm.dim_location     l  ON l.latitude IS NOT DISTINCT FROM e.latitude
                                AND l.longitude IS NOT DISTINCT FROM e.longitude
                                AND l.place = COALESCE(NULLIF(TRIM(e.place), ''), 'unknown')
LEFT JOIN dm.dim_magnitude    m  ON m.mag_type                                      = COALESCE(NULLIF(TRIM(e.mag_type), ''), 'unknown')
                                AND m.mag_source                                    = COALESCE(NULLIF(TRIM(e.mag_source), ''), 'unknown')
LEFT JOIN dm.dim_network      n  ON n.net                                           = COALESCE(NULLIF(TRIM(e.net), ''), 'unknown')
                                AND n.location_source                               = COALESCE(NULLIF(TRIM(e.location_source), ''), 'unknown')
LEFT JOIN dm.dim_status       s  ON s.status                                        = COALESCE(NULLIF(TRIM(e.status), ''), 'unknown')
LEFT JOIN dm.dim_event_type   et ON et.event_type                                   = COALESCE(NULLIF(TRIM(e.event_type), ''), 'unknown')
WHERE CAST(e.event_time AS DATE) = '{{ ds }}'
ON CONFLICT (id) DO UPDATE SET
    time_id       = EXCLUDED.time_id,
    location_id   = EXCLUDED.location_id,
    mag_type_id   = EXCLUDED.mag_type_id,
    network_id    = EXCLUDED.network_id,
    mag           = EXCLUDED.mag,
    depth         = EXCLUDED.depth,
    nst           = EXCLUDED.nst,
    gap           = EXCLUDED.gap,
    dmin          = EXCLUDED.dmin,
    rms           = EXCLUDED.rms,
    horizontal_error = EXCLUDED.horizontal_error,
    depth_error      = EXCLUDED.depth_error,
    mag_error        = EXCLUDED.mag_error,
    mag_nst          = EXCLUDED.mag_nst,
    status_id     = EXCLUDED.status_id,
    event_type_id = EXCLUDED.event_type_id,
    dwh_loaded_at = NOW();
"""

# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule=None,  # Запускается триггером из stg_to_ods_earthquake
    tags=["dm", "pg"],
    max_active_runs=1,
    catchup=True,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # Dimensions
    load_dim_region = SQLExecuteQueryOperator(
        task_id="load_dim_region",
        conn_id="postgres_dwh",
        sql=SQL_DIM_REGION,
    )

    # Dimensions — часть выполняется параллельно
    load_dim_time = SQLExecuteQueryOperator(
        task_id="load_dim_time",
        conn_id="postgres_dwh",
        sql=SQL_DIM_TIME,
    )
    load_dim_location = SQLExecuteQueryOperator(
        task_id="load_dim_location",
        conn_id="postgres_dwh",
        sql=SQL_DIM_LOCATION,
    )
    load_dim_magnitude = SQLExecuteQueryOperator(
        task_id="load_dim_magnitude",
        conn_id="postgres_dwh",
        sql=SQL_DIM_MAGNITUDE,
    )
    load_dim_network = SQLExecuteQueryOperator(
        task_id="load_dim_network",
        conn_id="postgres_dwh",
        sql=SQL_DIM_NETWORK,
    )
    load_dim_status = SQLExecuteQueryOperator(
        task_id="load_dim_status",
        conn_id="postgres_dwh",
        sql=SQL_DIM_STATUS,
    )
    load_dim_event_type = SQLExecuteQueryOperator(
        task_id="load_dim_event_type",
        conn_id="postgres_dwh",
        sql=SQL_DIM_EVENT_TYPE,
    )

    # Fact — только после всех dimensions
    load_fact = SQLExecuteQueryOperator(
        task_id="load_fact_earthquakes",
        conn_id="postgres_dwh",
        sql=SQL_FACT,
    )


    # ── Dependencies ──────────────────────────────────────────────────────────
    base_dims = [
        load_dim_time,
        load_dim_magnitude,
        load_dim_network,
        load_dim_status,
        load_dim_event_type,
    ]

    start >> load_dim_region # type: ignore
    start >> base_dims # type: ignore
    load_dim_region >> load_dim_location # type: ignore

    all_dims = [load_dim_region, load_dim_location, *base_dims]
    for dim in all_dims:
        dim >> load_fact # type: ignore
    load_fact >> end # type: ignore
