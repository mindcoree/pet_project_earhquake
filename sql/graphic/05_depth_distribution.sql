SELECT
    CASE
        WHEN f.depth < 10 THEN '0-10 km'
        WHEN f.depth < 30 THEN '10-30 km'
        WHEN f.depth < 70 THEN '30-70 km'
        WHEN f.depth < 150 THEN '70-150 km'
        ELSE '150+ km'
    END AS depth_bucket,
    COUNT(*) AS events_cnt
FROM dm.fact_earthquakes f
JOIN dm.dim_time t ON t.time_id = f.time_id
WHERE f.depth IS NOT NULL
[[ AND t.event_time::date >= {{date_from}} ]]
[[ AND t.event_time::date <= {{date_to}} ]]
GROUP BY 1
ORDER BY 1;
