-- https://docs.aws.amazon.com/athena/latest/ug/partitions.html
CREATE EXTERNAL TABLE taxi_rides (
    id                  STRING,
    vendor_id           INT,
    pickup_datetime     TIMESTAMP,
    dropoff_datetime    TIMESTAMP,
    passenger_count     INT,
    pickup_longitude    STRING,
    pickup_latitude     STRING,
    dropoff_longitude   STRING,
    dropoff_latitude    STRING,
    store_and_fwd_flag  STRING,
    gc_distance         INT,
    trip_duration       INT,
    google_distance     INT,
    google_duration     INT
) 
PARTITIONED BY (year STRING, month STRING, date STRING, hour STRING)
STORED AS parquet
LOCATION 's3://real-time-streaming-ap-southeast-2/taxi-rides/';

MSCK REPAIR TABLE taxi_rides;

SELECT * FROM taxi_rides WHERE year='2023';