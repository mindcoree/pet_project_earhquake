SELECT
    EXTRACT(ISODOW FROM t.event_time)::int AS day_of_week,
    EXTRACT(HOUR FROM t.event_time)::int AS hour_utc,
    COUNT(*)::int AS events_cnt
FROM dm.fact_earthquakes f
JOIN dm.dim_time t ON t.time_id = f.time_id
WHERE 1 = 1
[[ AND t.event_time::date >= {{date_from}} ]]
[[ AND t.event_time::date <= {{date_to}} ]]
GROUP BY 1, 2
ORDER BY 1, 2;
