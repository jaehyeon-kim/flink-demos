## Foundations

- [SQL Cookbook](https://github.com/ververica/flink-sql-cookbook)
- [Flink Faker](https://github.com/knaufk/flink-faker)
- [Data Faker](https://www.datafaker.net/documentation/getting-started/)

### 1. Creating Tables

```sql
CREATE TABLE orders (
  order_uid   BIGINT,
  product_id  BIGINT,
  price       DECIMAL(32,2),
  order_time  TIMESTAMP(3)
) WITH (
  'connector' = 'datagen'
);

SELECT * FROM orders;

SHOW TABLES;
DROP TABLE IF EXISTS orders;
```

### 2. Inserting Into Tables

```sql
CREATE TABLE server_logs (
  client_ip       STRING,
  client_identity STRING,
  user_id         STRING,
  user_agent      STRING,
  log_time        TIMESTAMP(3),
  request_line    STRING,
  status_code     STRING,
  size            INT
) WITH (
  'connector' = 'faker',
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.user_id.expression' =  '-',
  'fields.user_agent.expression' = '#{Internet.userAgent}',
  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',
  'fields.size.expression' = '#{number.numberBetween ''100'',''10000000''}'
);

CREATE TABLE client_errors (
  log_time      TIMESTAMP(3),
  request_line  STRING,
  status_code   STRING,
  size          INT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO client_errors
  SELECT
    log_time,
    request_line,
    status_code,
    size
  FROM server_logs
  WHERE
    status_code SIMILAR TO '4[0-9][0-9]';

DROP TABLE client_errors;
DROP TABLE server_logs;
```

### 3. Working with Temporary Tables

Non-temporary tables in Flink SQL are stored in a catalog, while temporary tables only live within the current session. You can use a temporary table instead of a regular (catalog) table, if it is only meant to be used within the current session or script.

```sql
CREATE TEMPORARY TABLE server_logs (
  client_ip       STRING,
  client_identity STRING,
  user_id         STRING,
  user_agent      STRING,
  log_time        TIMESTAMP(3),
  request_line    STRING,
  status_code     STRING,
  size            INT
) WITH (
  'connector' = 'faker',
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.user_id.expression' =  '-',
  'fields.user_agent.expression' = '#{Internet.userAgent}',
  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',
  'fields.size.expression' = '#{number.numberBetween ''100'',''10000000''}'
);

CREATE TEMPORARY TABLE client_errors (
  log_time      TIMESTAMP(3),
  request_line  STRING,
  status_code   STRING,
  size          INT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO client_errors
  SELECT
    log_time,
    request_line,
    status_code,
    size
  FROM server_logs
  WHERE
    status_code SIMILAR TO '4[0-9][0-9]';

DROP TEMPORARY TABLE client_errors;
DROP TEMPORARY TABLE server_logs;
```

### 4. Filtering Data

```sql
CREATE TABLE server_logs (
  client_ip       STRING,
  client_identity STRING,
  user_id         STRING,
  user_agent      STRING,
  log_time        TIMESTAMP(3),
  request_line    STRING,
  status_code     STRING,
  size            INT
) WITH (
  'connector' = 'faker',
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.user_id.expression' =  '-',
  'fields.user_agent.expression' = '#{Internet.userAgent}',
  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',
  'fields.size.expression' = '#{number.numberBetween ''100'',''10000000''}'
);

SELECT
  log_time,
  request_line,
  status_code
FROM server_logs
WHERE
  status_code in ('403', '401');

DROP TABLE server_logs;
```

### 5. Aggregating Data

```sql
CREATE TABLE server_logs (
  client_ip       STRING,
  client_identity STRING,
  user_id         STRING,
  user_agent      STRING,
  log_time        TIMESTAMP(3),
  request_line    STRING,
  status_code     STRING,
  size            INT
) WITH (
  'connector' = 'faker',
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.user_id.expression' =  '-',
  'fields.user_agent.expression' = '#{Internet.userAgent}',
  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',
  'fields.size.expression' = '#{number.numberBetween ''100'',''10000000''}'
);

-- Sample user_agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A
-- Regex pattern: '[^\/]+' (Match everything before '/')
SELECT
  REGEXP_EXTRACT(user_agent,'[^\/]+') AS browser,
  status_code,
  COUNT(*) AS cnt_status
FROM server_logs
GROUP BY
  REGEXP_EXTRACT(user_agent,'[^\/]+'),
  status_code;
```

### 6. Sorting Tables

- Bounded Tables can be sorted by any column, descending or ascending.
- For unbounded tables like server_logs, the primary sorting key needs to be a [time attribute](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/concepts/time_attributes/).
  - event time or process time

```sql
CREATE TABLE server_logs (
  client_ip       STRING,
  client_identity STRING,
  user_id         STRING,
  user_agent      STRING,
  log_time        TIMESTAMP(3),
  request_line    STRING,
  status_code     STRING,
  size            INT,
  WATERMARK FOR log_time AS log_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'faker',
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.user_id.expression' =  '-',
  'fields.user_agent.expression' = '#{Internet.userAgent}',
  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',
  'fields.size.expression' = '#{number.numberBetween ''100'',''10000000''}'
);

SELECT * FROM server_logs
ORDER BY log_time;

SELECT
  TUMBLE_ROWTIME(log_time, INTERVAL '5' SECOND) AS window_time,
  REGEXP_EXTRACT(user_agent,'[^\/]+') AS browser,
  COUNT(*) AS cnt_browser
FROM server_logs
GROUP BY
  REGEXP_EXTRACT(user_agent,'[^\/]+'),
  TUMBLE(log_time, INTERVAL '5' SECOND)
ORDER BY
  window_time,
  cnt_browser DESC;

SELECT
  window_start,
  window_end,
  REGEXP_EXTRACT(user_agent, '[^\/]+') AS browser,
  COUNT(*) AS cnt_browser
FROM TABLE(
  TUMBLE(TABLE server_logs, DESCRIPTOR(log_time), INTERVAL '5' SECOND)
)
GROUP BY
  window_start,
  window_end,
  REGEXP_EXTRACT(user_agent, '[^\/]+');
```

### 7. Encapsulating Logic with (Temporary) Views

- `CREATE (TEMPORARY) VIEW` defines a view from a query. A view is not physically materialized. Instead, the query is run every time the view is referenced in a query.

```sql
CREATE TABLE server_logs (
  client_ip       STRING,
  client_identity STRING,
  user_id         STRING,
  user_agent      STRING,
  log_time        TIMESTAMP(3),
  request_line    STRING,
  status_code     STRING,
  size            INT
) WITH (
  'connector' = 'faker',
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.user_id.expression' =  '-',
  'fields.user_agent.expression' = '#{Internet.userAgent}',
  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',
  'fields.size.expression' = '#{number.numberBetween ''100'',''10000000''}'
);

CREATE VIEW successful_requests AS
  SELECT *
  FROM server_logs
  WHERE status_code SIMILAR TO '[2,3][0-9][0-9]';

SELECT * FROM successful_requests;
```

### 8. Writing Results into Multiple Tables

- [STATEMENT SET](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/sqlclient/) - execute multiple insert statements as a single Flink job

```sql
CREATE TABLE server_logs (
  client_ip       STRING,
  client_identity STRING,
  user_id         STRING,
  user_agent      STRING,
  log_time        TIMESTAMP(3),
  request_line    STRING,
  status_code     STRING,
  size            INT,
  WATERMARK FOR log_time AS log_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'faker',
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.user_id.expression' =  '-',
  'fields.user_agent.expression' = '#{Internet.userAgent}',
  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',
  'fields.size.expression' = '#{number.numberBetween ''100'',''10000000''}'
);

CREATE TABLE realtime (
  browser     STRING,
  status_code STRING,
  end_time    TIMESTAMP(3),
  requests    BIGINT NOT NULL
) WITH (
  'connector' = 'kafka',
  'topic' = 'browser-status-codes',
  'properties.bootstrap.servers' = 'kafka-0:9092',
  'properties.group.id' = 'browser-counts',
  'format' = 'json'
);

CREATE TABLE offline (
  browser     STRING,
  status_code STRING,
  dt          STRING,
  `hour`        STRING,
  `minute`      STRING,
  requests    BIGINT NOT NULL
) PARTITIONED BY (dt, `hour`, `minute`) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///tmp/offline',
  'sink.partition-commit.trigger' = 'partition-time',
  'format' = 'json'
);

-- shared view
CREATE VIEW browsers AS
  SELECT
    REGEXP_EXTRACT(user_agent, '[^\/]+') AS browser,
    status_code,
    log_time
  FROM server_logs;


EXECUTE STATEMENT SET
BEGIN
  INSERT INTO realtime
    SELECT
      browser,
      status_code,
      TUMBLE_ROWTIME(log_time, INTERVAL '5' SECOND) AS end_time,
      COUNT(*) AS requests
    FROM browsers
    GROUP BY
      browser,
      status_code,
      TUMBLE(log_time, INTERVAL '5' SECOND);
  INSERT INTO offline
    SELECT
      browser,
      status_code,
      DATE_FORMAT(TUMBLE_ROWTIME(log_time, INTERVAL '1' MINUTE), 'yyyy-MM-dd') AS dt,
      DATE_FORMAT(TUMBLE_ROWTIME(log_time, INTERVAL '1' MINUTE), 'HH') AS `hour`,
      DATE_FORMAT(TUMBLE_ROWTIME(log_time, INTERVAL '1' MINUTE), 'mm') AS `minute`,
      COUNT(*) AS requests
    FROM browsers
    GROUP BY
      browser,
      status_code,
      TUMBLE(log_time, INTERVAL '1' MINUTE);
END;
```

### 9. Convert timestamps with timezones

```sql
CREATE TABLE iot_status (
  device_ip       STRING,
  device_timezone STRING,
  iot_timestamp   TIMESTAMP(3),
  status_code     STRING
) WITH (
  'connector' = 'faker',
  'fields.device_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.device_timezone.expression' =  '#{regexify ''(America\/Los_Angeles|Europe\/Rome|Europe\/London|Australia\/Sydney){1}''}',
  'fields.iot_timestamp.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.status_code.expression' = '#{regexify ''(OK|KO|WARNING){1}''}',
  'rows-per-second' = '3'
);

SELECT
  device_ip,
  device_timezone,
  iot_timestamp,
  CONVERT_TZ(CAST(iot_timestamp AS string), device_timezone, 'UTC') AS iot_timestamp_utc,
  status_code
FROM iot_status;
```

## Aggregations and Analytics

- [Time Attributes](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/concepts/time_attributes/#introduction-to-time-attributes)
- [Timely Stream Processing](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/concepts/time/)
- [Windowing table-valued functions (Windowing TVFs)](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/sql/queries/window-tvf/)
- [Window Aggregation](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/sql/queries/window-agg/)

### 1. Aggregating Time Series Data

- This example will show how to aggregate time series data in real-time using a `TUMBLE` window.

```sql
CREATE TABLE server_logs (
  client_ip       STRING,
  client_identity STRING,
  user_id         STRING,
  request_line    STRING,
  status_code     STRING,
  size            INT,
  log_time AS PROCTIME()
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '1',
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.user_id.expression' =  '-',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',
  'fields.size.expression' = '#{number.numberBetween ''0'',''50''}'
);

SELECT
  window_start,
  window_end,
  COUNT(DISTINCT client_ip) AS ip_addresses
FROM TABLE(
  TUMBLE(TABLE server_logs, DESCRIPTOR(log_time), INTERVAL '5' SECOND)
)
GROUP BY
  window_start,
  window_end;

SELECT
  TUMBLE_PROCTIME(log_time, INTERVAL '5' SECOND) AS end_time,
  COUNT(DISTINCT client_ip) AS ip_addresses
FROM server_logs
GROUP BY
  TUMBLE(log_time, INTERVAL '5' SECOND);
```

### 2. Watermarks

- This example will show how to use `WATERMARK`s to work with timestamps in records.
- [Streaming Analytics](https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/streaming_analytics/)

```sql
CREATE TABLE doctor_sightings (
  doctor        STRING,
  sighting_time TIMESTAMP(3),
  WATERMARK FOR sighting_time AS sighting_time - INTERVAL '5' SECONDS
)
WITH (
  'connector' = 'faker',
  'fields.doctor.expression' = '#{dr_who.the_doctors}',
  'fields.sighting_time.expression' = '#{date.past ''5'',''SECONDS''}'
);

SELECT
  doctor,
  TUMBLE_ROWTIME(sighting_time, INTERVAL '10' SECOND) AS sighting_time,
  COUNT(*) AS sightings
FROM doctor_sightings
GROUP BY
  TUMBLE(sighting_time, INTERVAL '10' SECOND),
  doctor;
```

### 3. Analyzing Sessions in Time Series Data

- This example will show how to aggregate time-series data in real-time using a `SESSION` window.
- Unlike tumbling windows, session windows don't have a fixed duration and are tracked independently across keys (i.e. windows of different keys will have different durations).

```sql
CREATE TABLE server_logs (
  client_ip       STRING,
  client_identity STRING,
  user_id         STRING,
  log_time        TIMESTAMP(3),
  request_line    STRING,
  status_code     STRING,
  size            INT,
  WATERMARK FOR log_time AS log_time - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '5',
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.user_id.expression' =  '#{regexify ''(morsapaes|knauf|sjwiesman){1}''}',
  'fields.log_time.expression' =  '#{date.past ''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',
  'fields.size.expression' = '#{number.numberBetween ''0'',''50''}'
);

SELECT
  user_id,
  SESSION_START(log_time, INTERVAL '10' SECOND) AS session_beg,
  SESSION_END(log_time, INTERVAL '10' SECOND) AS session_end,
  COUNT(request_line) AS request_cnt
FROM server_logs
WHERE status_code = '403'
GROUP BY
  user_id,
  SESSION(log_time, INTERVAL '10' SECOND);
```

### 4. Rolling Aggregations on Time Series Data

- This example will show how to calculate an aggregate or cumulative value based on a group of rows using an OVER window. A typical use case are rolling aggregations.
- OVER window aggregates compute an aggregated value for every input row over a range of ordered rows. In contrast to GROUP BY aggregates, OVER aggregates do not reduce the number of result rows to a single row for every group. Instead, OVER aggregates produce an aggregated value for every input row.

```sql
CREATE TEMPORARY TABLE temperature_measurements (
  measurement_time  TIMESTAMP(3),
  city              STRING,
  temperature       FLOAT,
  WATERMARK FOR measurement_time AS measurement_time - INTERVAL '15' SECONDS
)
WITH (
  'connector' = 'faker',
  'rows-per-second' = '5',
  'fields.measurement_time.expression' = '#{date.past ''15'',''SECONDS''}',
  'fields.temperature.expression' = '#{number.numberBetween ''0'',''50''}',
  'fields.city.expression' = '#{regexify ''(Chicago|Munich|Berlin|Portland|Hangzhou|Seatle|Beijing|New York){1}''}'
);

SELECT
  measurement_time,
  city,
  temperature,
  AVG(CAST(temperature AS FLOAT)) OVER last_minute AS avg_temperature_minute,
  MAX(temperature) OVER last_minute AS min_temperature_minute,
  MIN(temperature) OVER last_minute AS max_temperature_minute,
  STDDEV(temperature) OVER last_minute AS stdev_temperature_minute
FROM temperature_measurements
WHERE city = 'Berlin'
WINDOW last_minute AS (
  PARTITION BY city
  ORDER BY measurement_time
  RANGE BETWEEN INTERVAL '1' MINUTE PRECEDING AND CURRENT ROW
);
```

### 5. Continuous Top-N

- This example will show how to continuously calculate the `Top-N` rows based on a given attribute, using an OVER window and the `ROW_NUMBER()` function.

```sql
CREATE TABLE spells_cast (
    wizard STRING,
    spell  STRING
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '5',
  'fields.wizard.expression' = '#{harry_potter.characters}',
  'fields.spell.expression' = '#{harry_potter.spells}'
);

SELECT wizard, spell, times_cast, rownum
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY wizard ORDER BY times_cast DESC) AS rownum
  FROM (
    SELECT wizard, spell, COUNT(*) AS times_cast FROM spells_cast GROUP BY wizard, spell
  )
)
WHERE rownum <= 2;

-- set 'execution.runtime-mode' = 'streaming';
set 'sql-client.execution.result-mode' = 'table';
set 'sql-client.execution.result-mode' = 'changelog';

WITH group_cte AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY wizard ORDER BY times_cast DESC) AS rownum
  FROM (
    SELECT wizard, spell, COUNT(*) AS times_cast FROM spells_cast GROUP BY wizard, spell
  )
)
SELECT wizard, spell, times_cast, rownum
FROM group_cte
WHERe rownum <= 2;

```

### 6. Deduplication

- This example will show how you can identify and filter out duplicates in a stream of events.

```sql
CREATE TABLE orders (
  id INT,
  order_time AS CURRENT_TIMESTAMP,
  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECONDS
)
WITH (
  'connector' = 'datagen',
  'rows-per-second'='10',
  'fields.id.kind'='random',
  'fields.id.min'='1',
  'fields.id.max'='100'
);

-- check for duplicates in the `orders` table
SELECT id AS order_id,
       COUNT(*) AS order_cnt
FROM orders o
GROUP BY id
HAVING COUNT(*) > 1;

-- use decuplication to keep only the latest record for each 'order_id'
SELECT
  order_id,
  order_time
FROM (
  SELECT
    id AS order_id,
    order_time,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY order_time) AS rownum
  FROM orders
)
WHERE rownum = 1;
```

### 7. Chained (Event) Time Windows

- This example will show how to efficiently aggregate time series data on two different levels of granularity.
- agg by shorter time interval becomes agg of longer time interval

```sql
CREATE TABLE server_logs (
  client_ip       STRING,
  client_identity STRING,
  user_id         STRING,
  log_time        TIMESTAMP(3),
  request_line    STRING,
  status_code     STRING,
  size            INT,
  WATERMARK FOR log_time AS log_time - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'faker',
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.user_id.expression' =  '#{regexify ''(morsapaes|knauf|sjwiesman){1}''}',
  'fields.log_time.expression' =  '#{date.past ''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',
  'fields.size.expression' = '#{number.numberBetween ''0'',''50''}'
);

CREATE TABLE avg_request_size_short (
  window_start TIMESTAMP(3),
  windoe_end TIMESTAMP(3),
  avg_size BIGINT
) WITH (
  'connector' = 'blackhole'
);

CREATE TABLE avg_request_size_long (
  window_start TIMESTAMP(3),
  windoe_end TIMESTAMP(3),
  avg_size BIGINT
) WITH (
  'connector' = 'blackhole'
);

CREATE VIEW server_logs_window_short AS
  SELECT
    window_start AS ws,
    window_end AS we,
    window_time AS wt,
    SUM(size) AS total_size,
    COUNT(*) AS num_requests
  FROM TABLE(
    TUMBLE(TABLE server_logs, DESCRIPTOR(log_time), INTERVAL '10' SECOND)
  )
  GROUP BY
    window_start,
    window_end,
    window_time;

CREATE VIEW server_logs_window_long AS
  SELECT
    window_start,
    window_end,
    SUM(total_size) AS total_size,
    SUM(num_requests) AS num_requests
  FROM TABLE(
    TUMBLE(TABLE server_logs_window_short, DESCRIPTOR(wt), INTERVAL '20' SECOND)
  )
  GROUP BY
    window_start,
    window_end;

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO avg_request_size_short
    SELECT ws, we, total_size/num_requests AS ave_size
    FROM server_logs_window_short;
  INSERT INTO avg_request_size_long
    SELECT window_start, window_end, total_size/num_requests AS ave_size
    FROM server_logs_window_long;
END;
```

### 8. Detecting Patterns with MATCH_RECOGNIZE

- This example will show how you can use Flink SQL to detect patterns in a stream of events with `MATCH_RECOGNIZE`.

```sql
CREATE TABLE subscriptions (
    id STRING,
    user_id INT,
    type STRING,
    start_date TIMESTAMP(3),
    end_date TIMESTAMP(3),
    payment_expiration TIMESTAMP(3),
    proc_time AS PROCTIME()
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '1',
  'fields.id.expression' = '#{Internet.uuid}',
  'fields.user_id.expression' = '#{number.numberBetween ''1'',''5''}',
  'fields.type.expression'= '#{regexify ''(basic|premium|platinum){1}''}',
  'fields.start_date.expression' = '#{date.past ''30'',''DAYS''}',
  'fields.end_date.expression' = '#{date.future ''15'',''DAYS''}',
  'fields.payment_expiration.expression' = '#{date.future ''365'',''DAYS''}'
);

SELECT
  *
FROM subscriptions
    MATCH_RECOGNIZE (
      PARTITION BY user_id
      ORDER BY proc_time
      MEASURES
        LAST(PREMIUM.type) AS premium_type,
        AVG(TIMESTAMPDIFF(DAY, PREMIUM.start_date, PREMIUM.end_date)) AS premium_avg_duration,
        FIRST(PREMIUM.start_date) AS pstart,
        LAST(PREMIUM.end_date) AS pend,
        FIRST(BASIC.start_date) AS bstart,
        LAST(BASIC.end_date) AS bend,
        BASIC.start_date AS downgrade_date
      ONE ROW PER MATCH
      AFTER MATCH SKIP PAST LAST ROW
      -- Pattern: one or more 'premium' or 'platqinum' subscription events (PREMIUM)
      --  followed by a 'basic' subscription event (BASIC) for the same 'user_id'
      PATTERN (PREMIUM+ BASIC)
      DEFINE PREMIUM AS PREMIUM.type IN ('premium', 'platinum'),
             BASIC AS BASIC.type = 'basic'
    )
WHERE user_id = 1;
```

- [MATCH_RECOGNIZE: where Flink SQL and Complex Event Processing meet](https://www.ververica.com/blog/match_recognize-where-flink-sql-and-complex-event-processing-meet)
  - [GitHub](https://github.com/ververica/sql-training/wiki/Pattern-Matching-with-MATCH_RECOGNIZE)
- [Webinar: Detecting row patterns with Flink SQL - Dawid Wysakowicz](https://www.youtube.com/watch?v=7b5jcxbiVpQ)
- [Top 10 Flink SQL queries to try in Amazon Kinesis Data Analytics Studio](https://aws.amazon.com/blogs/big-data/top-10-flink-sql-queries-to-try-in-amazon-kinesis-data-analytics-studio/)
- [CDC Stream Processing with Apache Flink](https://www.youtube.com/watch?v=K2ibvfmFh8Y)
  - [Slide](https://archive.fosdem.org/2023/schedule/event/fast_data_cdc_apache_flink/attachments/slides/5563/export/events/attachments/fast_data_cdc_apache_flink/slides/5563/Apache_Flink_CDC_Slides.pdf)

```sql
CREATE TABLE ticker (
  symbol  STRING,
  price   FLOAT,
  tax     FLOAT,
  rowtime TIMESTAMP(3),
  WATERMARK FOR rowtime AS rowtime - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '5',
  'fields.symbol.expression' =  '#{regexify ''(AAAA|BBBB|CCCC|DDDD|EEEE){1}''}',
  'fields.price.expression' = '#{number.numberBetween ''0'',''50''}',
  'fields.tax.expression' = '#{number.numberBetween ''0'',''5''}',
  'fields.rowtime.expression' =  '#{date.past ''5'',''SECONDS''}'
);

SELECT *
FROM ticker
    MATCH_RECOGNIZE (
      PARTITION BY symbol
      ORDER BY rowtime
      MEASURES
        START_ROW.rowtime AS start_ts,
        LAST(PRICE_DOWN.rowtime) AS bottom_ts,
        LAST(PRICE_UP.rowtime) AS end_ts
      ONE ROW PER MATCH
      AFTER MATCH SKIP TO LAST PRICE_UP
      PATTERN (START_ROW PRICE_DOWN+ PRICE_UP)
      DEFINE
        PRICE_DOWN AS
          (LAST(PRICE_DOWN.price, 1) IS NULL AND PRICE_DOWN.price < START_ROW.price) OR
            PRICE_DOWN.price < LAST(PRICE_DOWN.price, 1),
        PRICE_UP AS
          PRICE_UP.price > LAST(PRICE_DOWN.price, 1)
    ) MR
WHERE symbol = 'AAAA';

```

### 9. Maintaining Materialized Views with Change Data Capture (CDC) and Debezium

```sql

```

### 10. Hopping Time Windows

- This example will show how to calculate a moving average in real-time using a HOP window.

```sql
CREATE TABLE bids (
    bid_id STRING,
    currency_code STRING,
    bid_price DOUBLE,
    transaction_time TIMESTAMP(3),
    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'faker',
  'fields.bid_id.expression' = '#{Internet.UUID}',
  'fields.currency_code.expression' = '#{regexify ''(EUR|USD|CNY)''}',
  'fields.bid_price.expression' = '#{Number.randomDouble ''2'',''1'',''150''}',
  'fields.transaction_time.expression' = '#{date.past ''30'',''SECONDS''}',
  'rows-per-second' = '100'
);

SELECT window_start, window_end, currency_code, ROUND(AVG(bid_price), 2) AS moving_avg
FROM TABLE(
  HOP(TABLE bids, DESCRIPTOR(transaction_time), INTERVAL '30' SECONDS, INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, currency_code;
```

### 11. Window Top-N

- This example will show how to calculate the Top 3 suppliers who have the highest sales for every tumbling 5 minutes window.
- The difference between the regular Top-N and this Window Top-N, is that Window Top-N only emits final results, which is the total top N records at the end of the window.

```sql
CREATE TABLE orders (
    bidtime TIMESTAMP(3),
    price DOUBLE,
    item STRING,
    supplier STRING,
    WATERMARK FOR bidtime AS bidtime - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'faker',
  'fields.bidtime.expression' = '#{date.past ''30'',''SECONDS''}',
  'fields.price.expression' = '#{Number.randomDouble ''2'',''1'',''150''}',
  'fields.item.expression' = '#{Commerce.productName}',
  'fields.supplier.expression' = '#{regexify ''(Alice|Bob|Carol|Alex|Joe|James|Jane|Jack)''}',
  'rows-per-second' = '100'
);

SELECT *
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY price DESC) AS rownum
  FROM (
    SELECT window_start, window_end, supplier, ROUND(SUM(price), 2) AS price, COUNT(*) AS cnt
    FROM TABLE (
      TUMBLE(TABLE orders, DESCRIPTOR(bidtime), INTERVAL '10' SECONDS)
    )
    GROUP BY window_start, window_end, supplier
  )
)
WHERE rownum <= 3;
```

### 12. Retrieve previous row value without self-join

- This example will show how to retrieve the previous value and compute trends for a specific data partition.

```sql
CREATE TABLE stocks (
    stock_name STRING,
    stock_value double,
    log_time AS PROCTIME()
) WITH (
  'connector' = 'faker',
  'fields.stock_name.expression' = '#{regexify ''(Deja\ Brew|Jurassic\ Pork|Lawn\ \&\ Order|Pita\ Pan|Bread\ Pitt|Indiana\ Jeans|Thai\ Tanic){1}''}',
  'fields.stock_value.expression' =  '#{number.randomDouble ''2'',''10'',''20''}',
  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'rows-per-second' = '10'
);

WITH current_and_previous AS (
  SELECT
    stock_name,
    log_time,
    stock_value,
    lag(stock_value, 1) OVER (PARTITION BY stock_name ORDER BY log_time) AS lag_value
  FROM stocks
)
SELECT *,
  CASE
    WHEN stock_value > lag_value THEN '▲'
    WHEN stock_value < lag_value THEN '▼'
    ELSE '='
  END AS trendq
FROM current_and_previous;
```

## Other Built-in Functions & Operators

### 1. Working with Dates and Timestamps

- This example will show how to use [built-in date and time functions](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/functions/systemfunctions/#temporal-functions) to manipulate temporal fields.

- `TO_TIMESTAMP(string[, format])`: converts a STRING value to a TIMESTAMP using the specified format (default: 'yyyy-MM-dd HH:mm:ss')
- `FROM_UNIXTIME(numeric[, string])`: converts an epoch to a formatted STRING (default: 'yyyy-MM-dd HH:mm:ss')
- `DATE_FORMAT(timestamp, string)`: converts a TIMESTAMP to a STRING using the specified format
- `EXTRACT(timeintervalunit FROM temporal)`: returns a LONG extracted from the specified date part of a temporal field (e.g. DAY,MONTH,YEAR)
- `TIMESTAMPDIFF(unit, timepoint1, timepoint2)`: returns the number of time units (SECOND, MINUTE, HOUR, DAY, MONTH or YEAR) between timepoint1 and timepoint2
- `CURRENT_TIMESTAMP`: returns the current SQL timestamp (UTC)

```sql
CREATE TABLE subscriptions (
    id STRING,
    start_date INT,
    end_date INT,
    payment_expiration TIMESTAMP(3)
) WITH (
  'connector' = 'faker',
  'fields.id.expression' = '#{Internet.uuid}',
  'fields.start_date.expression' = '#{number.numberBetween ''1576141834'',''1607764234''}',
  'fields.end_date.expression' = '#{number.numberBetween ''1609060234'',''1639300234''}',
  'fields.payment_expiration.expression' = '#{date.future ''365'',''DAYS''}'
);

SELECT
  id,
  TO_TIMESTAMP(FROM_UNIXTIME(start_date)) AS start_date,
  TO_TIMESTAMP(FROM_UNIXTIME(end_date)) AS end_date,
  DATE_FORMAT(payment_expiration, 'YYYYww') AS exp_yweek,
  EXTRACT(DAY FROM payment_expiration) AS exp_date,     -- DAYOFMONTH(ts)
  EXTRACT(MONTH FROM payment_expiration) AS exp_month,  -- MONTH(ts)
  EXTRACT(YEAR FROM payment_expiration) AS exp_year,    -- YEAR(ts)
  TIMESTAMPDIFF(DAY, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)), payment_expiration) AS exp_days
FROM subscriptions;
-- WHERE TIMESTAMPDIFF(DAY, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)), payment_expiration) < 30;

```

### 2. Building the Union of Multiple Streams

- This example will show how you can use the set operation UNION ALL to combine several streams of data.
- For other set operations, see [this document](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/set-ops/).

```sql
CREATE TEMPORARY TABLE rickandmorty_visits (
    visitor STRING,
    location STRING,
    visit_time TIMESTAMP(3)
) WITH (
  'connector' = 'faker',
  'fields.visitor.expression' = '#{RickAndMorty.character}',
  'fields.location.expression' =  '#{RickAndMorty.location}',
  'fields.visit_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}'
);

CREATE TEMPORARY TABLE spaceagency_visits (
    spacecraft STRING,
    location STRING,
    visit_time TIMESTAMP(3)
) WITH (
  'connector' = 'faker',
  'fields.spacecraft.expression' = '#{Space.nasaSpaceCraft}',
  'fields.location.expression' =  '#{Space.star}',
  'fields.visit_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}'
);

CREATE TEMPORARY TABLE hitchhiker_visits (
    visitor STRING,
    starship STRING,
    location STRING,
    visit_time TIMESTAMP(3)
) WITH (
  'connector' = 'faker',
  'fields.visitor.expression' = '#{HitchhikersGuideToTheGalaxy.character}',
  'fields.starship.expression' = '#{HitchhikersGuideToTheGalaxy.starship}',
  'fields.location.expression' =  '#{HitchhikersGuideToTheGalaxy.location}',
  'fields.visit_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}'
);

SELECT visitor, '' AS spacecraft, location, visit_time, 'rickandmorty' AS source FROM rickandmorty_visits
UNION ALL
SELECT '', spacecraft, location, visit_time, 'spaceagency' FROM spaceagency_visits
UNION ALL
SELECT visitor, starship, location, visit_time, 'hitchhiker' FROM hitchhiker_visits;
```

### 3. Filtering out Late Data

- This example will show how to filter out late data using the `CURRENT_WATERMARK` function.

```sql
  -- 'rows-per-second' = '1',
  -- 'number-of-rows' = '10',
CREATE TABLE mobile_usage (
  activity    STRING,
  client_ip   STRING,
  ingest_time AS PROCTIME(),
  log_time    TIMESTAMP_LTZ(3),
  WATERMARK FOR log_time AS log_time - INTERVAL '15' SECONDS
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '50',
  'fields.activity.expression' = '#{regexify ''(open_push_message|discard_push_message|open_app|display_overview|change_settings)''}',
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.log_time.expression' =  '#{date.past ''45'',''10'',''SECONDS''}'
);

SELECT * FROM mobile_usage /*+ OPTIONS('number-of-rows'='10') */;

CREATE TABLE unique_users_per_window (
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  ip_addresses  BIGINT
) WITH (
  'connector' = 'blackhole'
);

CREATE TABLE late_usage_events (
  activity    STRING,
  client_ip   STRING,
  ingest_time TIMESTAMP_LTZ(3),
  log_time    TIMESTAMP_LTZ(3),
  current_watermark TIMESTAMP_LTZ(3)
) WITH (
  'connector' = 'blackhole'
);

SELECT *,
  CURRENT_WATERMARK(log_time) AS wm,
  log_time <= CURRENT_WATERMARK(log_time) AS is_late
FROM mobile_usage;

CREATE TEMPORARY VIEW mobile_data AS
  SELECT * FROM mobile_usage
  WHERE CURRENT_WATERMARK(log_time) IS NULL
    OR log_time > CURRENT_WATERMARK(log_time);

CREATE TEMPORARY VIEW late_mobile_data AS
  SELECT * FROM mobile_usage
  WHERE CURRENT_WATERMARK(log_time) IS NOT NULL
    AND log_time <= CURRENT_WATERMARK(log_time);

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO unique_users_per_window
    SELECT window_start, window_end, COUNT(DISTINCT client_ip) As ip_addresses
      FROM TABLE(
        TUMBLE(TABLE mobile_data, DESCRIPTOR(log_time), INTERVAL '1' MINUTE)
      )
      GROUP BY window_start, window_end;
  INSERT INTO late_usage_events
    SELECT *, CURRENT_WATERMARK(log_time) AS current_watermark FROM late_mobile_data;
END;
```

### 4. Overriding table options

- This example will show how you can override table options that have been defined via a DDL by using [SQL Hints](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/hints/).

```sql
CREATE TABLE `airports` (
  `IATA_CODE` CHAR(3),
  `AIRPORT` STRING,
  `CITY` STRING,
  `STATE` CHAR(2),
  `COUNTRY` CHAR(3),
  `LATITUDE` DOUBLE NULL,
  `LONGITUDE` DOUBLE NULL,
  PRIMARY KEY (`IATA_CODE`) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///tmp/src/airports.csv',
  'format' = 'csv'
);

SELECT * FROM airports;
-- java.lang.NumberFormatException: For input string: "LATITUDE"

SELECT * FROM airports /*+ OPTIONS('csv.ignore-parse-errors'='true') */;

SELECT * FROM airports /*+ OPTIONS('csv.ignore-parse-errors'='true') */
WHERE `LATITUDE` IS NULL;

-- can apply to other tables
SELECT * FROM `your_kafka_topic` /*+ OPTIONS('scan.startup.mode'='group-offsets') */;
SELECT * FROM `flink-faker-table` /*+ OPTIONS('number-of-rows'='10') */;
```

### 5. Expanding arrays into new rows

- This example will show how to create new rows for each element in an array using a `CROSS JOIN UNNEST`.

```sql
CREATE TABLE `HarryPotter` (
  `character` STRING,
  `spells` ARRAY<STRING>
) WITH (
  'connector' = 'faker',
  'fields.character.expression' = '#{harry_potter.character}',
  'fields.spells.expression' = '#{harry_potter.spell}',
  'fields.spells.length' = '3'
);

CREATE TABLE `SpellsLanguage` (
  `spells` STRING,
  `spoken_language` STRING,
  `proctime` AS PROCTIME()
)
WITH (
  'connector' = 'faker',
  'fields.spells.expression' = '#{harry_potter.spell}',
  'fields.spoken_language.expression' = '#{regexify ''(Parseltongue|Rune|Gobbledegook|Mermish|Troll|English)''}'
);

SELECT * FROM HarryPotter;

CREATE TEMPORARY VIEW `SpellsPerCharacter` AS
  SELECT `HarryPotter`.`character`, `SpellsTable`.`spell`
  FROM HarryPotter
  CROSS JOIN UNNEST(HarryPotter.spells) AS SpellsTable (spell);

SELECT
  `SpellsPerCharacter`.`character`,
  `SpellsPerCharacter`.`spell`,
  `SpellsLanguage`.`spoken_language`
FROM SpellsPerCharacter
JOIN SpellsLanguage FOR SYSTEM_TIME AS OF proctime AS SpellsLanguage
ON SpellsPerCharacter.spell = SpellsLanguage.spells;
```

### 6. Split strings into maps

- This example will show how you can create a map of key/value pairs by splitting string values using `STR_TO_MAP`.

```sql
CREATE TABLE `customers` (
  `identifier` STRING,
  `fullname` STRING,
  `postal_address` STRING,
  `residential_address` STRING,
  `k1` STRING,
  `k2` STRING
) WITH (
  'connector' = 'faker',
  'fields.identifier.expression' = '#{Internet.uuid}',
  'fields.fullname.expression' = '#{Name.firstName} #{Name.lastName}',
  'fields.postal_address.expression' = '#{Address.fullAddress}',
  'fields.residential_address.expression' = '#{Address.fullAddress}',
  'fields.k1.expression' = '#{regexify ''(2|4|3){1}''}',
  'fields.k2.expression' = '#{regexify ''(2|4|3){1}''}',
  'rows-per-second' = '1'
);

SELECT
  identifier,
  fullname,
  STR_TO_MAP('postal_address:' || postal_address || ';residential_address:' || residential_address, ';', ':') As addresses,
  STR_TO_MAP('k1:' || k1 || ';k2:' || k2, ';', ':') As keys
FROM customers;
```

### 7. Extract data from JSON

- This example will show how you can extract data from JSON using [JSON functions](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/functions/systemfunctions/#json-functions).

```sql
CREATE TABLE clients (
  `json_string` STRING
)
WITH (
  'connector' = 'faker',
  'fields.json_string.expression' = '#{jsona ''-1'',''id'',''#{Internet.uuid}'',''-1'', ''full_name'',''#{Name.full_name}'',''2'',''addresses'',''#{json ''''address'''',''''#{Address.fullAddress}''''}''}'
);

SELECT
  json_string IS JSON AS is_json,
  JSON_EXISTS(json_string, '$.id') AS is_id_exists,
  JSON_VALUE(json_string, '$.full_name') AS full_name,
  JSON_QUERY(json_string, '$.addresses[*].address') AS address,
  json_string
FROM clients;
```

### 8. Generate data from JSON

- - This example will show how you can generate JSON using [JSON functions](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/functions/systemfunctions/#json-functions).

```sql
CREATE TABLE customers (
  `prefix` STRING,
  `first_name` STRING,
  `last_name` STRING,
  `address` ARRAY<STRING>
)
WITH (
  'connector' = 'faker',
  'fields.first_name.expression' = '#{Name.first_name}',
  'fields.last_name.expression' = '#{Name.last_name}',
  'fields.address.expression' = '#{Address.full_address}',
  'fields.address.length' = '3',
  'fields.prefix.expression' = '#{Name.prefix}'
);

SELECT
  JSON_OBJECT(KEY 'first_name' VALUE first_name, KEY 'last_name' VALUE last_name, KEY 'address' VALUE address) AS obj,
  JSON_ARRAY(first_name, last_name) AS arr
FROM customers;

SELECT JSON_OBJECTAGG(KEY prefix VALUE cnt)
FROM (
  SELECT prefix, COUNT(1) AS cnt
  FROM customers
  GROUP BY prefix
) t
WHERE cnt > 1;

SELECT JSON_ARRAYAGG(prefix)
FROM (SELECT DISTINCT prefix FROM customers) t;
```

## User-Defined Functions (UDFs)

### 1. Extending SQL with Python UDFs

- This example will show how to extend Flink SQL with custom functions written in Python.

```py
# /tmp/src/python_udf.py
from pyflink.table import DataTypes
from pyflink.table.udf import udf

us_cities = {"Chicago", "Portland", "Seattle", "New York"}

@udf(input_types=[DataTypes.STRING(), DataTypes.FLOAT()], result_type=DataTypes.FLOAT())
def to_fahr(city, temperature):
    return temperature if city not in us_cities else (temperature * 9.0 / 5.0) + 32.0
```

```sql
--Register the Python UDF using the fully qualified
--name of the function ([module name].[object name])
CREATE FUNCTION to_fahr AS 'python_udf.to_fahr'
LANGUAGE PYTHON;

CREATE TABLE temperature_measurements (
  city STRING,
  temperature FLOAT,
  measurement_time TIMESTAMP(3),
  WATERMARK FOR measurement_time AS measurement_time - INTERVAL '15' SECONDS
)
WITH (
  'connector' = 'faker',
  'fields.temperature.expression' = '#{number.numberBetween ''0'',''42''}',
  'fields.measurement_time.expression' = '#{date.past ''15'',''SECONDS''}',
  'fields.city.expression' = '#{regexify ''(Copenhagen|Berlin|Chicago|Portland|Seattle|New York){1}''}'
);

SELECT
  city,
  temperature AS tmp,
  to_fahr(city, temperature) As tmp_conv,
  measurement_time
FROM temperature_measurements;
```

## Joins

### 1. Regular Joins

- This example will show how you can use joins to correlate rows across multiple tables.
- Note Flink retains every input row and can fail eventually

```sql
CREATE TABLE noc (
  agent_id STRING,
  codename STRING
) WiTH (
  'connector' = 'faker',
  'fields.agent_id.expression' = '#{regexify ''(1|2|3|4|5){1}''}',
  'fields.codename.expression' = '#{superhero.name}',
  'number-of-rows' = '10'
);

CREATE TABLE real_names (
  agent_id STRING,
  name     STRING
)
WITH (
  'connector' = 'faker',
  'fields.agent_id.expression' = '#{regexify ''(1|2|3|4|5){1}''}',
  'fields.name.expression' = '#{Name.full_name}',
  'number-of-rows' = '10'
);

SELECT n.agent_id, name, codename
FROM noc AS n
INNER JOIN real_names AS r ON n.agent_id = r.agent_id;
```

### 2. Interval Joins

- This example will show how you can perform joins between tables with events that are related in a temporal context.

```sql
CREATE TABLE orders (
  id INT,
  order_time AS TIMESTAMPADD(DAY, CAST(FLOOR(RAND()*(1-5+1)+5)*(-1) AS INT), CURRENT_TIMESTAMP)
)
WITH (
  'connector' = 'datagen',
  'rows-per-second'='10',
  'fields.id.kind'='sequence',
  'fields.id.start'='1',
  'fields.id.end'='1000'
);

CREATE TABLE shipments (
  id INT,
  order_id INT,
  shipment_time AS TIMESTAMPADD(DAY, CAST(FLOOR(RAND()*(1-5+1)) AS INT), CURRENT_TIMESTAMP)
)
WITH (
  'connector' = 'datagen',
  'rows-per-second'='5',
  'fields.id.kind'='random',
  'fields.id.min'='0',
  'fields.order_id.kind'='sequence',
  'fields.order_id.start'='1',
  'fields.order_id.end'='1000'
);

SELECT
  o.id AS order_id,
  o.order_time,
  s.shipment_time,
  TIMESTAMPDIFF(DAY,o.order_time,s.shipment_time) AS day_diff
FROM orders o
JOIN shipments s ON o.id = s.order_id
WHERE
    o.order_time BETWEEN s.shipment_time - INTERVAL '3' DAY AND s.shipment_time;
```

### 3. Temporal Table Join between a non-compacted and compacted Kafka Topic

- In this recipe, you will see how to correctly enrich records from one Kafka topic with the corresponding records of another Kafka topic when the order of events matters.
- Temporal table joins take an arbitrary table (left input/probe site) and correlate each row to the corresponding row’s relevant version in a versioned table (right input/build side). Flink uses the SQL syntax of FOR SYSTEM_TIME AS OF to perform this operation.

```sql
CREATE TEMPORARY TABLE currency_rates (
  `currency_code` STRING,
  `eur_rate` DECIMAL(6,4),
  `rate_time` TIMESTAMP(3),
  WATERMARK FOR `rate_time` AS rate_time - INTERVAL '15' SECOND,
  PRIMARY KEY (currency_code) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'currency_rates',
  'properties.bootstrap.servers' = 'kafka-0:9092',
  'key.format' = 'raw',
  'value.format' = 'json',
  'properties.allow.auto.create.topics' = 'true'
);

CREATE TEMPORARY TABLE currency_rates_faker
WITH (
  'connector' = 'faker',
  'fields.currency_code.expression' = '#{Currency.code}',
  'fields.eur_rate.expression' = '#{Number.randomDouble ''4'',''0'',''10''}',
  'fields.rate_time.expression' = '#{date.past ''15'',''SECONDS''}',
  'rows-per-second' = '100'
) LIKE currency_rates (EXCLUDING OPTIONS);

INSERT INTO currency_rates
  SELECT * FROM currency_rates_faker;

CREATE TEMPORARY TABLE transactions (
  `id` STRING,
  `currency_code` STRING,
  `total` DECIMAL(10,2),
  `transaction_time` TIMESTAMP(3),
  WATERMARK FOR `transaction_time` AS transaction_time - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'transactions',
  'properties.bootstrap.servers' = 'kafka-0:9092',
  'key.format' = 'raw',
  'key.fields' = 'id',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'properties.group.id' = 'transactions-group',
  'properties.allow.auto.create.topics' = 'true',
  'scan.startup.mode' = 'earliest-offset'
);

CREATE TEMPORARY TABLE transactions_faker
WITH (
  'connector' = 'faker',
  'fields.id.expression' = '#{Internet.UUID}',
  'fields.currency_code.expression' = '#{Currency.code}',
  'fields.total.expression' = '#{Number.randomDouble ''2'',''10'',''1000''}',
  'fields.transaction_time.expression' = '#{date.past ''30'',''SECONDS''}',
  'rows-per-second' = '100'
) LIKE transactions (EXCLUDING OPTIONS);

INSERT INTO transactions
  SELECT * FROM transactions_faker;

SELECT
  t.id,
  t.total * c.eur_rate AS total_eur,
  t.total,
  c.currency_code,
  t.transaction_time,
  c.rate_time
FROM transactions t
JOIN currency_rates FOR SYSTEM_TIME AS OF t.transaction_time AS c
ON t.currency_code = c.currency_code
WHERE t.currency_code = 'AUD';
```

### 4. Lookup Joins

- This example will show how you can enrich a stream with an external table of reference data (i.e. a lookup table).
- The join requires one table to have a processing time attribute and the other table to be backed by a lookup source connector, like the JDBC connector.

```sql
CREATE TABLE subscriptions (
    id STRING,
    user_id INT,
    type STRING,
    start_date TIMESTAMP(3),
    end_date TIMESTAMP(3),
    payment_expiration TIMESTAMP(3),
    proc_time AS PROCTIME()
) WITH (
  'connector' = 'faker',
  'fields.id.expression' = '#{Internet.uuid}',
  'fields.user_id.expression' = '#{number.numberBetween ''1'',''50''}',
  'fields.type.expression'= '#{regexify ''(basic|premium|platinum){1}''}',
  'fields.start_date.expression' = '#{date.past ''30'',''DAYS''}',
  'fields.end_date.expression' = '#{date.future ''365'',''DAYS''}',
  'fields.payment_expiration.expression' = '#{date.future ''365'',''DAYS''}'
);

CREATE TABLE users (
  user_id   INT,
  user_name STRING
)
WITH (
  'connector' = 'faker',
  'fields.user_id.expression' = '#{number.numberBetween ''0'',''50''}',
  'fields.user_name.expression' = '#{harry_potter.characters}'
);

SELECT
  s.id AS subscription_id,
  s.type AS subscription_type,
  s.user_id,
  u.user_name
FROM subscriptions s
JOIN users FOR SYSTEM_TIME AS OF s.proc_time AS u
  ON s.user_id = u.user_id;
```

### 5. Star Schema Denormalization (N-Way Join)

- In this recipe, we will de-normalize a simple star schema with an n-way temporal table join.

```sql
CREATE TEMPORARY TABLE passengers (
  passenger_key STRING,
  first_name STRING,
  last_name STRING,
  update_time TIMESTAMP(3),
  WATERMARK FOR update_time AS update_time - INTERVAL '10' SECONDS,
  PRIMARY KEY (passenger_key) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'passengers',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'raw',
  'value.format' = 'json',
  'properties.allow.auto.create.topics' = 'true'
);

CREATE TEMPORARY TABLE passengers_faker
WITH (
  'connector' = 'faker',
  'fields.passenger_key.expression' = '#{number.numberBetween ''0'',''10000000''}',
  'fields.update_time.expression' = '#{date.past ''10'',''5'',''SECONDS''}',
  'fields.first_name.expression' = '#{Name.firstName}',
  'fields.last_name.expression' = '#{Name.lastName}',
  'rows-per-second' = '1000'
) LIKE passengers (EXCLUDING OPTIONS);

CREATE TEMPORARY TABLE stations (
  station_key STRING,
  update_time TIMESTAMP(3),
  city STRING,
  WATERMARK FOR update_time AS update_time - INTERVAL '10' SECONDS,
  PRIMARY KEY (station_key) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'stations',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'raw',
  'value.format' = 'json',
  'properties.allow.auto.create.topics' = 'true'
);

CREATE TEMPORARY TABLE stations_faker
WITH (
  'connector' = 'faker',
  'fields.station_key.expression' = '#{number.numberBetween ''0'',''1000''}',
  'fields.city.expression' = '#{Address.city}',
  'fields.update_time.expression' = '#{date.past ''10'',''5'',''SECONDS''}',
  'rows-per-second' = '100'
) LIKE stations (EXCLUDING OPTIONS);

CREATE TEMPORARY TABLE booking_channels (
  booking_channel_key STRING,
  update_time TIMESTAMP(3),
  channel STRING,
  WATERMARK FOR update_time AS update_time - INTERVAL '10' SECONDS,
  PRIMARY KEY (booking_channel_key) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'booking_channels',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'raw',
  'value.format' = 'json',
  'properties.allow.auto.create.topics' = 'true'
);

CREATE TEMPORARY TABLE booking_channels_faker
WITH (
  'connector' = 'faker',
  'fields.booking_channel_key.expression' = '#{number.numberBetween ''0'',''7''}',
  'fields.channel.expression' = '#{regexify ''(bahn\.de|station|retailer|app|lidl|hotline|joyn){1}''}',
  'fields.update_time.expression' = '#{date.past ''10'',''5'',''SECONDS''}',
  'rows-per-second' = '100'
) LIKE booking_channels (EXCLUDING OPTIONS);

CREATE TEMPORARY TABLE train_activities (
  scheduled_departure_time TIMESTAMP(3),
  actual_departure_date TIMESTAMP(3),
  passenger_key STRING,
  origin_station_key STRING,
  destination_station_key STRING,
  booking_channel_key STRING,
  WATERMARK FOR actual_departure_date AS actual_departure_date - INTERVAL '10' SECONDS
) WITH (
  'connector' = 'kafka',
  'topic' = 'train_activities',
  'properties.bootstrap.servers' = 'localhost:9092',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'properties.group.id' = 'train-activities-group',
  'properties.allow.auto.create.topics' = 'true',
  'scan.startup.mode' = 'earliest-offset'
);

CREATE TEMPORARY TABLE train_activities_faker
WITH (
  'connector' = 'faker',
  'fields.scheduled_departure_time.expression' = '#{date.past ''10'',''0'',''SECONDS''}',
  'fields.actual_departure_date.expression' = '#{date.past ''10'',''5'',''SECONDS''}',
  'fields.passenger_key.expression' = '#{number.numberBetween ''0'',''10000000''}',
  'fields.origin_station_key.expression' = '#{number.numberBetween ''0'',''1000''}',
  'fields.destination_station_key.expression' = '#{number.numberBetween ''0'',''1000''}',
  'fields.booking_channel_key.expression' = '#{number.numberBetween ''0'',''7''}',
  'rows-per-second' = '1000'
) LIKE train_activities (EXCLUDING OPTIONS);

INSERT INTO passengers SELECT * FROM passengers_faker;
INSERT INTO stations SELECT * FROM stations_faker;
INSERT INTO booking_channels SELECT * FROM booking_channels_faker;
INSERT INTO train_activities SELECT * FROM train_activities_faker;

SELECT
  t.actual_departure_date,
  p.first_name,
  p.last_name,
  b.channel,
  os.city AS origin_station,
  ds.city AS destination_station
FROM train_activities t
LEFT JOIN booking_channels FOR SYSTEM_TIME AS OF t.actual_departure_date AS b
ON t.booking_channel_key = b.booking_channel_key;
LEFT JOIN passengers FOR SYSTEM_TIME AS OF t.actual_departure_date AS p
ON t.passenger_key = p.passenger_key
LEFT JOIN stations FOR SYSTEM_TIME AS OF t.actual_departure_date AS os
ON t.origin_station_key = os.station_key
LEFT JOIN stations FOR SYSTEM_TIME AS OF t.actual_departure_date AS ds
ON t.destination_station_key = ds.station_key;
```

### 6. Lateral Table Join

- This example will show how you can correlate events using a LATERAL join.

```sql
CREATE TABLE people (
    id           INT,
    city         STRING,
    state        STRING,
    arrival_time TIMESTAMP(3),
    WATERMARK FOR arrival_time AS arrival_time - INTERVAL '1' MINUTE
) WITH (
    'connector' = 'faker',
    'fields.id.expression'    = '#{number.numberBetween ''1'',''100''}',
    'fields.city.expression'  = '#{regexify ''(Newmouth|Newburgh|Portport|Southfort|Springfield){1}''}',
    'fields.state.expression' = '#{regexify ''(New York|Illinois|California|Washington){1}''}',
    'fields.arrival_time.expression' = '#{date.past ''15'',''SECONDS''}',
    'rows-per-second'          = '10'
);

CREATE TEMPORARY VIEW current_population AS
  SELECT city, state, COUNT(*) AS population
  FROM (
    SELECT city, state, ROW_NUMBER() OVER (PARTITION BY id ORDER BY arrival_time DESC) AS rownum
    FROM people
  )
  WHERE rownum = 1
  GROUP BY city, state;

SELECT
    state,
    city,
    population
FROM
    (SELECT DISTINCT state FROM current_population) states,
    LATERAL (
        SELECT city, population
        FROM current_population
        WHERE state = states.state
        ORDER BY population DESC
        LIMIT 2
)
where state = 'California';
```
