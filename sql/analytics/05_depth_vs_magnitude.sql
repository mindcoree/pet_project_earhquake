-- Depth vs magnitude buckets

SELECT
    CASE
        WHEN f.depth < 10 THEN '0-10 km'
        WHEN f.depth < 30 THEN '10-30 km'
        WHEN f.depth < 70 THEN '30-70 km'
        WHEN f.depth < 150 THEN '70-150 km'
        ELSE '150+ km'
    END AS depth_bucket,
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
WHERE f.depth IS NOT NULL
  AND f.mag IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;
