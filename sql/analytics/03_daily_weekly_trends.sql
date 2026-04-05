-- Daily and weekly trend examples

-- Daily trend
SELECT
    t.event_time::date AS day,
    COUNT(*) AS events_cnt,
    ROUND(AVG(f.mag)::numeric, 2) AS avg_mag
FROM dm.fact_earthquakes f
JOIN dm.dim_time t ON t.time_id = f.time_id
GROUP BY 1
ORDER BY 1;

-- Weekly trend
SELECT
    date_trunc('week', t.event_time)::date AS week_start,
    COUNT(*) AS events_cnt,
    ROUND(AVG(f.mag)::numeric, 2) AS avg_mag
FROM dm.fact_earthquakes f
JOIN dm.dim_time t ON t.time_id = f.time_id
GROUP BY 1
ORDER BY 1;
