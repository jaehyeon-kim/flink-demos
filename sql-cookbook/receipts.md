## Foundations

- [SQL Cookbook](https://github.com/ververica/flink-sql-cookbook)
- [Flink Faker](https://github.com/knaufk/flink-faker)
- [Data Faker](https://www.datafaker.net/documentation/getting-started/)

1. Creating Tables

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

2. Inserting Into Tables

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

3. Working with Temporary Tables

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

4. Filtering Data

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

5. Aggregating Data

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

6. Sorting Tables

- Bounded Tables can be sorted by any column, descending or ascending.
- For unbounded tables like server_logs, the primary sorting key needs to be a [time attribute](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/concepts/time_attributes/).
  - either event time or process time

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
```

7. Encapsulating Logic with (Temporary) Views

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

CREATE VIEW successful_requests AS
  SELECT *
  FROM server_logs
  WHERE status_code SIMILAR TO '[2,3][0-9][0-9]';

SELECT * FROM successful_requests;
```

8. Writing Results into Multiple Tables

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

9. Convert timestamps with timezones

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

1. Aggregating Time Series Data
2. Watermarks
3. Analyzing Sessions in Time Series Data
4. Rolling Aggregations on Time Series Data
5. Continuous Top-N
6. Deduplication
7. Chained (Event) Time Windows
8. Detecting Patterns with MATCH_RECOGNIZE
9. Maintaining Materialized Views with Change Data Capture (CDC) and Debezium
10. Hopping Time Windows
11. Window Top-N
12. Retrieve previous row value without self-join

## Other Built-in Functions & Operators

1. Working with Dates and Timestamps
2. Building the Union of Multiple Streams
3. Filtering out Late Data
4. Overriding table options
5. Expanding arrays into new rows
6. Split strings into maps

## User-Defined Functions (UDFs)

1. Extending SQL with Python UDFs

## Joins

1. Regular Joins
2. Interval Joins
3. Temporal Table Join between a non-compacted and compacted Kafka Topic
4. Lookup Joins
5. Star Schema Denormalization (N-Way Join)
6. Lateral Table Join
