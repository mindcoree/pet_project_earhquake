-- Data quality checks for DM layer

-- 1) ODS vs FACT row count
SELECT
    (SELECT COUNT(*) FROM ods.earthquakes) AS ods_cnt,
    (SELECT COUNT(*) FROM dm.fact_earthquakes) AS fact_cnt;

-- 2) Events present in ODS but missing in FACT
SELECT COUNT(*) AS missing_in_fact
FROM ods.earthquakes e
LEFT JOIN dm.fact_earthquakes f ON f.id = e.id
WHERE f.id IS NULL;

-- 3) NULL foreign keys in FACT
SELECT COUNT(*) AS fact_fk_nulls
FROM dm.fact_earthquakes
WHERE time_id IS NULL
   OR location_id IS NULL
   OR mag_type_id IS NULL
   OR network_id IS NULL
   OR status_id IS NULL
   OR event_type_id IS NULL;
