-- Uniqueness checks in dimensions

-- 1) dim_magnitude uniqueness by (mag_type, mag_source)
SELECT
    mag_type,
    mag_source,
    COUNT(*) AS cnt
FROM dm.dim_magnitude
GROUP BY 1, 2
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

-- 2) dim_network uniqueness by (net, location_source)
SELECT
    net,
    location_source,
    COUNT(*) AS cnt
FROM dm.dim_network
GROUP BY 1, 2
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

-- 3) orphaned dimension keys usage check (optional):
-- dimensions with no references from fact
SELECT 'dim_status' AS dim_name, COUNT(*) AS orphan_rows
FROM dm.dim_status s
LEFT JOIN dm.fact_earthquakes f ON f.status_id = s.status_id
WHERE f.status_id IS NULL
UNION ALL
SELECT 'dim_event_type' AS dim_name, COUNT(*) AS orphan_rows
FROM dm.dim_event_type et
LEFT JOIN dm.fact_earthquakes f ON f.event_type_id = et.event_type_id
WHERE f.event_type_id IS NULL;
