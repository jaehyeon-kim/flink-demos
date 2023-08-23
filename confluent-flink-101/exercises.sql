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



--//
--// Implementing and Troubleshooting Watermarks (Exercise)
--//


--//
--// Experiencing Failure Recovery (Exercise)
--//