-- Event type and status analysis

SELECT
    et.event_type,
    s.status,
    COUNT(*) AS events_cnt,
    ROUND(AVG(f.mag)::numeric, 2) AS avg_mag
FROM dm.fact_earthquakes f
JOIN dm.dim_event_type et ON et.event_type_id = f.event_type_id
JOIN dm.dim_status s ON s.status_id = f.status_id
GROUP BY 1, 2
ORDER BY events_cnt DESC, avg_mag DESC;
