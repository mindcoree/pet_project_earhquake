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

# ── Sub-dimensions ────────────────────────────────────────────────────────────

SQL_DIM_REGION = """
INSERT INTO dm.dim_region (region_name)
SELECT DISTINCT region
FROM ods.earthquakes
WHERE CAST(event_time AS DATE) = '{{ ds }}'
  AND region IS NOT NULL
ON CONFLICT (region_name) DO NOTHING;
"""

SQL_DIM_MAG_SOURCE = """
INSERT INTO dm.dim_mag_source (mag_source)
SELECT DISTINCT mag_source
FROM ods.earthquakes
WHERE CAST(event_time AS DATE) = '{{ ds }}'
  AND mag_source IS NOT NULL
ON CONFLICT (mag_source) DO NOTHING;
"""

SQL_DIM_LOCATION_SOURCE = """
INSERT INTO dm.dim_location_source (location_source)
SELECT DISTINCT location_source
FROM ods.earthquakes
WHERE CAST(event_time AS DATE) = '{{ ds }}'
  AND location_source IS NOT NULL
ON CONFLICT (location_source) DO NOTHING;
"""

# ── Dimensions ────────────────────────────────────────────────────────────────

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
    e.place
FROM ods.earthquakes e
LEFT JOIN dm.dim_region r ON r.region_name = e.region
WHERE CAST(e.event_time AS DATE) = '{{ ds }}'
ON CONFLICT (latitude, longitude, place) DO NOTHING;
"""

SQL_DIM_MAGNITUDE = """
INSERT INTO dm.dim_magnitude (mag_source_id, mag_type)
SELECT DISTINCT
    ms.mag_source_id,
    e.mag_type
FROM ods.earthquakes e
LEFT JOIN dm.dim_mag_source ms ON ms.mag_source = e.mag_source
WHERE CAST(e.event_time AS DATE) = '{{ ds }}'
  AND e.mag_type IS NOT NULL
ON CONFLICT (mag_type) DO NOTHING;
"""

SQL_DIM_NETWORK = """
INSERT INTO dm.dim_network (loc_source_id, net)
SELECT DISTINCT
    ls.loc_source_id,
    e.net
FROM ods.earthquakes e
LEFT JOIN dm.dim_location_source ls ON ls.location_source = e.location_source
WHERE CAST(e.event_time AS DATE) = '{{ ds }}'
  AND e.net IS NOT NULL
ON CONFLICT (net) DO NOTHING;
"""

SQL_DIM_STATUS = """
INSERT INTO dm.dim_status (status)
SELECT DISTINCT status
FROM ods.earthquakes
WHERE CAST(event_time AS DATE) = '{{ ds }}'
  AND status IS NOT NULL
ON CONFLICT (status) DO NOTHING;
"""

SQL_DIM_EVENT_TYPE = """
INSERT INTO dm.dim_event_type (event_type)
SELECT DISTINCT event_type
FROM ods.earthquakes
WHERE CAST(event_time AS DATE) = '{{ ds }}'
  AND event_type IS NOT NULL
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
LEFT JOIN dm.dim_location     l  ON l.latitude = e.latitude
                                AND l.longitude = e.longitude
                                AND l.place = e.place
LEFT JOIN dm.dim_magnitude    m  ON m.mag_type                                      = e.mag_type
LEFT JOIN dm.dim_network      n  ON n.net                                           = e.net
LEFT JOIN dm.dim_status       s  ON s.status                                        = e.status
LEFT JOIN dm.dim_event_type   et ON et.event_type                                   = e.event_type
WHERE CAST(e.event_time AS DATE) = '{{ ds }}'
ON CONFLICT (id) DO UPDATE SET
    time_id       = EXCLUDED.time_id,
    location_id   = EXCLUDED.location_id,
    mag           = EXCLUDED.mag,
    status_id     = EXCLUDED.status_id,
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

    # Sub-dimensions — параллельно, ни от чего не зависят
    load_dim_region = SQLExecuteQueryOperator(
        task_id="load_dim_region",
        conn_id="postgres_dwh",
        sql=SQL_DIM_REGION,
    )
    load_dim_mag_source = SQLExecuteQueryOperator(
        task_id="load_dim_mag_source",
        conn_id="postgres_dwh",
        sql=SQL_DIM_MAG_SOURCE,
    )
    load_dim_location_source = SQLExecuteQueryOperator(
        task_id="load_dim_location_source",
        conn_id="postgres_dwh",
        sql=SQL_DIM_LOCATION_SOURCE,
    )

    # Dimensions — параллельно, но после sub-dimensions
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
    #
    #  start
    #    ├── load_dim_region ──────────────────────┐
    #    ├── load_dim_mag_source ──────────────────┤
    #    └── load_dim_location_source ─────────────┤
    #                                              ▼
    #                              ┌── load_dim_time
    #                              ├── load_dim_location    (нужен dim_region)
    #                              ├── load_dim_magnitude   (нужен dim_mag_source)
    #                              ├── load_dim_network     (нужен dim_location_source)
    #                              ├── load_dim_status
    #                              └── load_dim_event_type
    #                                              │
    #                                              ▼
    #                                       load_fact
    #                                              │
    #                                             end

    sub_dims = [load_dim_region, load_dim_mag_source, load_dim_location_source]
    dims     = [load_dim_time, load_dim_location, load_dim_magnitude,
                load_dim_network, load_dim_status, load_dim_event_type]
 
    start >> sub_dims # type: ignore
    for sub in sub_dims:
        sub >> dims # type: ignore
    for dim in dims:
        dim >> load_fact # type: ignore
    load_fact >> end # type: ignore


