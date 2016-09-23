CREATE MATERIALIZED VIEW IF NOT EXISTS my_ks.sensor_by_type
AS SELECT type,sensor_id,date,value
FROM my_ks.sensor
WHERE type IS NOT NULL AND sensor_id IS NOT NULL AND date IS NOT NULL
PRIMARY KEY((type),sensor_id,date)
WITH CLUSTERING ORDER BY(sensor_id ASC,date ASC);