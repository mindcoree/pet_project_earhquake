SELECT
    r.region_name,
    COUNT(*) AS events_cnt,
    ROUND(AVG(f.mag)::numeric, 2) AS avg_mag
FROM dm.fact_earthquakes f
JOIN dm.dim_location l ON l.location_id = f.location_id
JOIN dm.dim_region r ON r.region_id = l.region_id
JOIN dm.dim_time t ON t.time_id = f.time_id
WHERE 1 = 1
[[ AND t.event_time::date >= {{date_from}} ]]
[[ AND t.event_time::date <= {{date_to}} ]]
GROUP BY 1
HAVING COUNT(*) >= 10
ORDER BY events_cnt DESC
LIMIT 20;
