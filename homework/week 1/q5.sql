SELECT z."Zone", SUM(g.total_amount) AS total_amount
FROM green_tripdata g
JOIN taxi_zone_lookup z
ON g."PULocationID" = z."LocationID"
WHERE DATE(lpep_pickup_datetime) = '2025-11-18'
GROUP BY z."Zone"
ORDER BY total_amount DESC
LIMIT 1;
