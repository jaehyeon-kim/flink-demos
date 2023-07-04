--//
--// Experiment with Batch and Streaming
--//
CREATE TABLE `bounded_pageviews` (
  `url` STRING,
  `user_id` STRING,
  `browser` STRING,
  `ts` TIMESTAMP(3)
)
WITH (
  'connector' = 'faker',
  'number-of-rows' = '500', --// bounded
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