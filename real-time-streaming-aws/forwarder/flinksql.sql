-- docker exec -it jobmanager ./bin/sql-client.sh

SET 'state.checkpoints.dir' = 'file:///tmp/checkpoints/';
SET 'execution.checkpointing.interval' = '5000';

ADD JAR '/etc/flink/package/lib/lab4-pipeline-1.0.0.jar';

CREATE TABLE taxi_rides_src (
    id                  VARCHAR,
    vendor_id           INT,
    pickup_date         VARCHAR,
    dropoff_date        VARCHAR,
    passenger_count     INT,
    pickup_longitude    VARCHAR,
    pickup_latitude     VARCHAR,
    dropoff_longitude   VARCHAR,
    dropoff_latitude    VARCHAR,
    store_and_fwd_flag  VARCHAR,
    gc_distance         INT,
    trip_duration       INT,
    google_distance     INT,
    google_duration     INT,
    process_time        AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'taxi-rides',
    'properties.bootstrap.servers' = 'kafka-0:9092',
    'properties.group.id' = 'soruce-group',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE taxi_rides_sink (
    vendor_id           VARCHAR, 
    trip_count          BIGINT NOT NULL,
    passenger_count     INT,
    trip_duration       INT,
    window_start        TIMESTAMP(3) NOT NULL,
    window_end          TIMESTAMP(3) NOT NULL
) WITH (
    'connector' = 'opensearch',
    'hosts' = 'http://opensearch:9200',
    'index' = 'trip_stats'
);

INSERT INTO taxi_rides_sink
SELECT 
    CAST(vendor_id AS STRING) AS vendor_id,
    COUNT(id) AS trip_count,
    SUM(passenger_count) AS passenger_count,
    SUM(trip_duration) AS trip_duration,
    window_start,
    window_end
FROM TABLE(
   TUMBLE(TABLE taxi_rides_src, DESCRIPTOR(process_time), INTERVAL '5' SECONDS))
GROUP BY vendor_id, window_start, window_end;