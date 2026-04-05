SELECT
    CASE
        WHEN f.mag < 2 THEN '<2'
        WHEN f.mag < 3 THEN '2-3'
        WHEN f.mag < 4 THEN '3-4'
        WHEN f.mag < 5 THEN '4-5'
        WHEN f.mag < 6 THEN '5-6'
        ELSE '6+'
    END AS mag_bucket,
    COUNT(*) AS events_cnt
FROM dm.fact_earthquakes f
JOIN dm.dim_time t ON t.time_id = f.time_id
WHERE f.mag IS NOT NULL
[[ AND t.event_time::date >= {{date_from}} ]]
[[ AND t.event_time::date <= {{date_to}} ]]
GROUP BY 1
ORDER BY 1;
