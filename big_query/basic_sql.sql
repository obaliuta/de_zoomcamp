-- Drop the existing regular table
--DROP TABLE `trips_data_all.rides`;

-- Now create the external table
CREATE EXTERNAL TABLE `trips_data_all.yellow_rides`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://trips_raw_data/data/yellow/yellow_tripdata_2020-*.parquet',
    'gs://trips_raw_data/data/yellow/yellow_tripdata_2021-*.parquet'
  ]
);

SELECT * FROM trips_data_all.yellow_rides limit 10;


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE trips_data_all.yellow_tripdata_non_partitioned AS
SELECT * FROM trips_data_all.yellow_rides ;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE trips_data_all.yellow_tripdata_partitioned 
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM trips_data_all.yellow_rides ;


-- Impact of partition
-- Scanning 1.6GB of data
SELECT DISTINCT(VendorID)
FROM trips_data_all.yellow_tripdata_non_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-01' AND '2021-03-31';

-- Scanning ~106 MB of DATA
SELECT DISTINCT(VendorID)
FROM trips_data_all.yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-01' AND '2021-03-31'



-- Let's look into the partitions
SELECT table_name, partition_id, total_rows
FROM `trips_data_all.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitioned'
ORDER BY total_rows DESC;


CREATE OR REPLACE TABLE trips_data_all.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID
AS
SELECT 
  CAST(VendorID AS INT64) AS VendorID,
  -- Select all other columns as-is
  *
EXCEPT (VendorID)
FROM 
  trips_data_all.yellow_rides;


-- Query scans 1.1 GB
SELECT count(*) as trips
FROM trips_data_all.yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-01' AND '2021-03-31'
  AND VendorID=1;

-- Query scans 864.5 MB
SELECT count(*) as trips
FROM trips_data_all.yellow_tripdata_partitioned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-01' AND '2021-03-31'
  AND VendorID=1;
