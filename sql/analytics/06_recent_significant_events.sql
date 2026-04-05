-- Recent significant events

SELECT
    t.event_time,
    r.region_name,
    l.place,
    f.mag,
    f.depth,
    n.net,
    n.location_source,
    s.status,
    et.event_type
FROM dm.fact_earthquakes f
JOIN dm.dim_time t ON t.time_id = f.time_id
JOIN dm.dim_location l ON l.location_id = f.location_id
JOIN dm.dim_region r ON r.region_id = l.region_id
JOIN dm.dim_network n ON n.network_id = f.network_id
JOIN dm.dim_status s ON s.status_id = f.status_id
JOIN dm.dim_event_type et ON et.event_type_id = f.event_type_id
WHERE f.mag >= 5
ORDER BY t.event_time DESC
LIMIT 200;
