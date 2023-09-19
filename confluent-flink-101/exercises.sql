--//
--// Batch and Stream Processing with Flink SQL (Exercise)
--// Using the Flink Web UI (Exercise)
--//

-- Streaming mode, bounded input
CREATE TABLE `bounded_pageviews` (
  `url` STRING,
  `user_id` STRING,
  `browser` STRING,
  `ts` TIMESTAMP(3)
)
WITH (
  'connector' = 'faker',
  'number-of-rows' = '500',
  'rows-per-second' = '100',
  'fields.url.expression' = '/#{superhero.name}.html',
  'fields.user_id.expression' = '#{numerify ''user_##''}',
  'fields.browser.expression' = '#{Options.option ''chrome'', ''firefox'', ''safari'')}',
  'fields.ts.expression' =  '#{date.past ''5'',''1'',''SECONDS''}'
);

select * from bounded_pageviews limit 10;

--// Batch mode, bounded input
set 'execution.runtime-mode' = 'batch';
select count(*) AS `count` from bounded_pageviews;

--// Streaming mode, bounded input
set 'execution.runtime-mode' = 'streaming';
set 'sql-client.execution.result-mode' = 'changelog';
select count(*) AS `count` from bounded_pageviews;

--// Streaming mode, unbounded input
CREATE TABLE `pageviews` (
  `url` STRING,
  `user_id` STRING,
  `browser` STRING,
  `ts` TIMESTAMP(3)
)
WITH (
  'connector' = 'faker',
  'rows-per-second' = '100',
  'fields.url.expression' = '/#{superhero.name}.html',
  'fields.user_id.expression' = '#{numerify ''user_##''}',
  'fields.browser.expression' = '#{Options.option ''chrome'', ''firefox'', ''safari'')}',
  'fields.ts.expression' =  '#{date.past ''5'',''1'',''SECONDS''}'
);

set 'sql-client.execution.result-mode' = 'changelog';
select count(*) AS `count` from pageviews;

set 'sql-client.execution.result-mode' = 'table';
select count(*) AS `count` from pageviews;

ALTER TABLE `pageviews` SET ('rows-per-second' = '10');

EXPLAIN select count(*) from pageviews;

--//
--// Deploying an ETL Pipeline using Flink SQL (Exercise)
--//
DROP TABLE IF EXISTS pageviews;
SHOW TABLES;

CREATE TABLE `pageviews` (
  `url` STRING,
  `user_id` STRING,
  `browser` STRING,
  `ts` TIMESTAMP(3)
)
WITH (
  'connector' = 'faker',
  'rows-per-second' = '10',
  'fields.url.expression' = '/#{superhero.name}.html',
  'fields.user_id.expression' = '#{numerify ''user_##''}',
  'fields.browser.expression' = '#{Options.option ''chrome'', ''firefox'', ''safari'')}',
  'fields.ts.expression' =  '#{date.past ''5'',''1'',''SECONDS''}'
);

CREATE TABLE pageviews_kafka (
  `url` STRING,
  `user_id` STRING,
  `browser` STRING,
  `ts` TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'pageviews',
  'properties.bootstrap.servers' = 'kafka-0:9092',
  'properties.group.id' = 'pageviews-demo',
  'scan.startup.mode' = 'earliest-offset',  
  'format' = 'json',
  'sink.partitioner' = 'fixed'
);

INSERT INTO pageviews_kafka
  SELECT * FROM pageviews;

SELECT * FROM pageviews_kafka;
SELECT browser, COUNT(*) FROM pageviews_kafka GROUP BY browser;

--//
--// Streaming Analytics with Flink SQL (Exercise)
--//
-- Three families of SQL operators
--  - Stateless: SELECT (projection/transformation), WHeRE (filter)
--  - Materializing (dangerously stateful): regular GROUP BY aggregations, regular JOINs (note tables are unbounded)
--  - Temporal (sately stateful): time-windowed aggregations, interval joins, time-versioned joins, MATCH_RECOGNIZE (pattern matching)

CREATE TABLE `pageviews` (
  `url` STRING,
  `user_id` STRING,
  `browser` STRING,
  `ts` TIMESTAMP(3)
)
WITH (
  'connector' = 'faker',
  'rows-per-second' = '100',
  'fields.url.expression' = '/#{superhero.name}.html',
  'fields.user_id.expression' = '#{numerify ''user_##''}',
  'fields.browser.expression' = '#{Options.option ''chrome'', ''firefox'', ''safari'')}',
  'fields.ts.expression' =  '#{date.past ''5'',''1'',''SECONDS''}'
);

-- native approach to windowing with Flink SQL
--  - rounding the timestamp in each page view down to the nearest second, state has to be kept indefinitely
SELECT 
  FLOOR(ts to SECOND) AS window_start,
  count(url) AS cnt
FROM pageviews
GROUP BY FLOOR(ts to SECOND);

--  - techinically same as below
SELECT
  browser,
  count(url) as cnt
FROM pageviews
GROUP BY browser;

-- better approach to windowing using time attributes and table-valued functions
--  - requirements of windowing (i.e. windows that will be cleaned up once there're no longer changing)
--  - 1. an input table that is append-only
--  - 2. a designated timestamp column with timestamps that are known to be advancing
--  - 
--  - time attribute can be either processing time <- advancing or event time <- needs watermarks

DROP TABLE IF EXISTS pageviews;
CREATE TABLE `pageviews` (
  `url` STRING,
  `user_id` STRING,
  `browser` STRING,
  `ts` TIMESTAMP(3),
  `proc_time`AS PROCTIME()
)
WITH (
  'connector' = 'faker',
  'rows-per-second' = '1',
  'fields.url.expression' = '/#{superhero.name}.html',
  'fields.user_id.expression' = '#{numerify ''user_##''}',
  'fields.browser.expression' = '#{Options.option ''chrome'', ''firefox'', ''safari'')}',
  'fields.ts.expression' =  '#{date.past ''5'',''1'',''SECONDS''}'
);

DESCRIBE pageviews;

SELECT
  window_start, window_end, window_time, count(url) AS cnt
FROM TABLE(TUMBLE(TABLE pageviews, DESCRIPTOR(proc_time), INTERVAL '3' SECOND))
GROUP BY window_start, window_end, window_time;

SELECT *
FROM TABLE(TUMBLE(TABLE pageviews, DESCRIPTOR(proc_time), INTERVAL '3' SECOND));

-- Supported windows (https://nightlqies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/window-tvf/)
--    Tumble Windows
--    Hop Windows
--    Cumulate Windows
--    Session Windows (will be supported soon)
--      Group Window Aggregation is deprecated - https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/window-agg/

--//
--// Implementing and Troubleshooting Watermarks (Exercise)
--//
DROP TABLE IF EXISTS pageviews;
CREATE TABLE `pageviews` (
  `url` STRING,
  `user_id` STRING,
  `browser` STRING,
  `ts` TIMESTAMP(3),
  WATERMARK FOR `ts` AS ts - INTERVAL '1' SECOND
)
WITH (
  'connector' = 'faker',
  'rows-per-second' = '1',
  'fields.url.expression' = '/#{superhero.name}.html',
  'fields.user_id.expression' = '#{numerify ''user_##''}',
  'fields.browser.expression' = '#{Options.option ''chrome'', ''firefox'', ''safari'')}',
  'fields.ts.expression' =  '#{date.past ''15'',''1'',''SECONDS''}'
);

set 'sql-client.execution.result-mode' = 'changelog';

-- tumbling window by event time with watermarks
SELECT
  window_start, window_end, count(url) AS cnt
FROM TABLE(
  TUMBLE(TABLE pageviews, DESCRIPTOR(ts), INTERVAL '5' SECOND))
GROUP BY window_start, window_end;

SELECT ts, window_start, window_end
FROM TABLE(TUMBLE(TABLE pageviews, DESCRIPTOR(ts), INTERVAL '5' SECOND));

-- pattern match
-- finds cases where users had two pageview events using two different browsers within one second
SELECT *
FROM pageviews
  MATCH_RECOGNIZE (
    PARTITION BY user_id
    ORDER BY ts
    MEASURES
      A.browser AS browser1,
      B.browser AS browser2,
      A.ts AS ts1,
      B.ts AS ts2
    PATTERN (A B) WITHIN INTERVAL '1' SECOND
    DEFINE
     A AS true,
     B AS B.browser <> A.browser
  );

--//
--// Experiencing Failure Recovery (Exercise)
--//
-- a checkpoint is an automatic snapshot created by Flink, primarily for the purpose of failure recovery
-- a savepoint is a manual snaphot created for some operational purpose (e.g. stateful upgrade, version upgrade...)

-- because of self-consistent, global snapshots
--  - flink provides (effetively) exactly-once guarantees
--  - recovery involves restarting the entire cluster from the most recent snapshot 
--  - <-- the whole cluster, not just the node that failed...

set 'execution.checkpointing.interval' = '1000';
set 'sql-client.execution.result-mode' = 'changelog';

ALTER TABLE `pageviews` SET ('rows-per-second' = '1');

SELECT COUNT(*) FROM pageviews;

-- docker-compose kill taskmanager
-- docker-compose up --no-deps -d taskmanager
