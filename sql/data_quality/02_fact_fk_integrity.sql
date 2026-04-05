-- Foreign key integrity checks for dm.fact_earthquakes

-- 1) Count rows with NULL FK values (target: 0)
SELECT COUNT(*) AS fact_fk_nulls
FROM dm.fact_earthquakes
WHERE time_id IS NULL
   OR location_id IS NULL
   OR mag_type_id IS NULL
   OR network_id IS NULL
   OR status_id IS NULL
   OR event_type_id IS NULL;

-- 2) Show affected rows if any
SELECT
    id,
    time_id,
    location_id,
    mag_type_id,
    network_id,
    status_id,
    event_type_id
FROM dm.fact_earthquakes
WHERE time_id IS NULL
   OR location_id IS NULL
   OR mag_type_id IS NULL
   OR network_id IS NULL
   OR status_id IS NULL
   OR event_type_id IS NULL
ORDER BY id;

-- 3) Ensure FK keys resolve to dimension rows (target: 0)
SELECT COUNT(*) AS broken_dim_links
FROM dm.fact_earthquakes f
LEFT JOIN dm.dim_time t ON t.time_id = f.time_id
LEFT JOIN dm.dim_location l ON l.location_id = f.location_id
LEFT JOIN dm.dim_magnitude m ON m.mag_type_id = f.mag_type_id
LEFT JOIN dm.dim_network n ON n.network_id = f.network_id
LEFT JOIN dm.dim_status s ON s.status_id = f.status_id
LEFT JOIN dm.dim_event_type et ON et.event_type_id = f.event_type_id
WHERE t.time_id IS NULL
   OR l.location_id IS NULL
   OR m.mag_type_id IS NULL
   OR n.network_id IS NULL
   OR s.status_id IS NULL
   OR et.event_type_id IS NULL;
