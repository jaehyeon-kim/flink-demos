--// source table
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

--// sink table
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

SHOW TABLES;

INSERT INTO pageviews_kafka SELECT * FROM pageviews;

select count(*) from pageviews;
select count(*) from pageviews_kafka;

SELECT * FROM pageviews_kafka;
SELECT browser, COUNT(*) FROM pageviews_kafka GROUP BY browser;

-- set 'execution.runtime-mode' = 'streaming';
-- set 'sql-client.execution.result-mode' = 'table';