-- Reconciliation checks between ODS and DM fact layer

-- 1) Total count comparison
SELECT
    (SELECT COUNT(*) FROM ods.earthquakes) AS ods_cnt,
    (SELECT COUNT(*) FROM dm.fact_earthquakes) AS fact_cnt,
    (SELECT COUNT(*) FROM ods.earthquakes) - (SELECT COUNT(*) FROM dm.fact_earthquakes) AS diff_cnt;

-- 2) Events in ODS missing in FACT
SELECT COUNT(*) AS missing_in_fact
FROM ods.earthquakes e
LEFT JOIN dm.fact_earthquakes f ON f.id = e.id
WHERE f.id IS NULL;

-- 3) Daily reconciliation (diff should be 0 for each date)
WITH ods_daily AS (
    SELECT CAST(event_time AS DATE) AS dt, COUNT(*) AS cnt
    FROM ods.earthquakes
    GROUP BY 1
),
fact_daily AS (
    SELECT CAST(t.event_time AS DATE) AS dt, COUNT(*) AS cnt
    FROM dm.fact_earthquakes f
    JOIN dm.dim_time t ON t.time_id = f.time_id
    GROUP BY 1
)
SELECT
    COALESCE(o.dt, f.dt) AS dt,
    COALESCE(o.cnt, 0) AS ods_cnt,
    COALESCE(f.cnt, 0) AS fact_cnt,
    COALESCE(o.cnt, 0) - COALESCE(f.cnt, 0) AS diff
FROM ods_daily o
FULL JOIN fact_daily f ON f.dt = o.dt
WHERE COALESCE(o.cnt, 0) <> COALESCE(f.cnt, 0)
ORDER BY 1;
