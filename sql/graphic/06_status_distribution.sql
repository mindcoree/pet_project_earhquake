SELECT
    s.status,
    COUNT(*) AS events_cnt
FROM dm.fact_earthquakes f
JOIN dm.dim_status s ON s.status_id = f.status_id
JOIN dm.dim_time t ON t.time_id = f.time_id
WHERE 1 = 1
[[ AND t.event_time::date >= {{date_from}} ]]
[[ AND t.event_time::date <= {{date_to}} ]]
GROUP BY 1
ORDER BY events_cnt DESC;
