WITH daily AS (
    SELECT
        t.event_time::date AS day,
        COUNT(*) AS events_cnt
    FROM dm.fact_earthquakes f
    JOIN dm.dim_time t ON t.time_id = f.time_id
    WHERE 1 = 1
    [[ AND t.event_time::date >= {{date_from}} ]]
    [[ AND t.event_time::date <= {{date_to}} ]]
    GROUP BY 1
)
SELECT
    day,
    events_cnt,
    ROUND(AVG(events_cnt) OVER (
        ORDER BY day
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS ma7
FROM daily
ORDER BY day;
