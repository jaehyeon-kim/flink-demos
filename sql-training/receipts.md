## Apache Flink® SQL Training

- [GitHub](https://github.com/ververica/sql-training/tree/master)

### Sessions

```sql
CREATE TABLE rides (
  rideId INT,
  taxiId INT,
  isStart BOOLEAN,
  lon FLOAT,
  lat FLOAT,
  psgCnt INT,
  eventTime STRING,
  rideTime AS TO_TIMESTAMP(eventTime, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
  WATERMARK FOR rideTime AS rideTime - INTERVAL '60' SECOND
)
WITH (
  'connector' = 'kafka',
  'topic' = 'Rides',
  'properties.bootstrap.servers' = 'kafka-0:9092',
  'properties.group.id' = 'rides',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE fairs (
  rideId INT,
  payMethod STRING,
  tip FLOAT,
  toll FLOAT,
  fare FLOAT,
  eventTime STRING,
  payTime AS TO_TIMESTAMP(eventTime, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
  WATERMARK FOR payTime AS payTime - INTERVAL '60' SECOND
)
WITH (
  'connector' = 'kafka',
  'topic' = 'Fares',
  'properties.bootstrap.servers' = 'kafka-0:9092',
  'properties.group.id' = 'fares',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE driver_changes (
  taxiId INT,
  driverId INT,
  eventTime STRING,
  usageStartTime AS TO_TIMESTAMP(eventTime, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
  WATERMARK FOR usageStartTime AS usageStartTime - INTERVAL '60' SECOND
)
WITH (
  'connector' = 'kafka',
  'topic' = 'DriverChanges',
  'properties.bootstrap.servers' = 'kafka-0:9092',
  'properties.group.id' = 'driver-changes',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

#### Introduction to SQL on Flink

#### Querying Dynamic Tables with SQL

#### Queries and Time

#### Joining Dynamic Tables

#### Pattern Matching with MATCH_RECOGNIZE

#### Creating Tables & Writing Query Results to External Systems
