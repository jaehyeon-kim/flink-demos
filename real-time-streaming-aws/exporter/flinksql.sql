-- docker exec -it jobmanager ./bin/sql-client.sh

SET 'state.checkpoints.dir' = 'file:///tmp/checkpoints/';
SET 'execution.checkpointing.interval' = '60000';

ADD JAR '/etc/lib/kafka-clients-3.2.3.jar';
ADD JAR '/etc/flink/package/lib/lab3-pipeline-1.0.0.jar';

CREATE TABLE taxi_rides_src (
    id                  VARCHAR,
    vendor_id           INT,
    pickup_date         VARCHAR,
    pickup_datetime     AS TO_TIMESTAMP(REPLACE(pickup_date, 'T', ' ')),
    dropoff_date        VARCHAR,
    dropoff_datetime    AS TO_TIMESTAMP(REPLACE(dropoff_date, 'T', ' ')),
    passenger_count     INT,
    pickup_longitude    VARCHAR,
    pickup_latitude     VARCHAR,
    dropoff_longitude   VARCHAR,
    dropoff_latitude    VARCHAR,
    store_and_fwd_flag  VARCHAR,
    gc_distance         INT,
    trip_duration       INT,
    google_distance     INT,
    google_duration     INT
) WITH (
    'connector' = 'kafka',
    'topic' = 'taxi-rides',
    'properties.bootstrap.servers' = 'kafka-0:9092',
    'properties.group.id' = 'soruce-group',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE taxi_rides_sink (
    id                  VARCHAR,
    vendor_id           INT,
    pickup_datetime     TIMESTAMP,
    dropoff_datetime    TIMESTAMP,
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
    `year`              VARCHAR,
    `month`             VARCHAR,
    `date`              VARCHAR,
    `hour`              VARCHAR
) PARTITIONED BY (`year`, `month`, `date`, `hour`) WITH (
    'connector' = 'filesystem',
    'path' = 's3://real-time-streaming-ap-southeast-2/taxi-rides/',
    'format' = 'parquet',
    'sink.partition-commit.delay'='1 h',
    'sink.partition-commit.policy.kind'='success-file'
);

-- 'path' = '/tmp/taxi_rides',

INSERT INTO taxi_rides_sink
SELECT
  id,
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  pickup_longitude,
  pickup_latitude,
  dropoff_longitude,
  dropoff_latitude,
  store_and_fwd_flag,
  gc_distance,
  trip_duration,
  google_distance,
  google_duration,
  DATE_FORMAT(pickup_datetime, 'yyyy') AS `year`,
  DATE_FORMAT(pickup_datetime, 'MM') AS `month`,
  DATE_FORMAT(pickup_datetime, 'dd') AS `date`,
  DATE_FORMAT(pickup_datetime, 'HH') AS `hour`
FROM taxi_rides_src;