-- Top regions and magnitude metrics

SELECT
    r.region_name,
    COUNT(*) AS events_cnt,
    ROUND(AVG(f.mag)::numeric, 2) AS avg_mag,
    MAX(f.mag) AS max_mag,
    COUNT(*) FILTER (WHERE f.mag >= 5) AS events_mag_5_plus
FROM dm.fact_earthquakes f
JOIN dm.dim_location l ON l.location_id = f.location_id
JOIN dm.dim_region r ON r.region_id = l.region_id
GROUP BY 1
HAVING COUNT(*) >= 10
ORDER BY events_cnt DESC, avg_mag DESC
LIMIT 25;
