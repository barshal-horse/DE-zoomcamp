SELECT z2."Zone", MAX(g.tip_amount) AS max_tip
FROM green_tripdata g
JOIN taxi_zone_lookup z1
  ON g."PULocationID" = z1."LocationID"
JOIN taxi_zone_lookup z2
  ON g."DOLocationID" = z2."LocationID"
WHERE z1."Zone" = 'East Harlem North'
  AND lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
GROUP BY z2."Zone"
ORDER BY max_tip DESC
LIMIT 1;
