-- docker exec -it jobmanager bin/sql-client.sh --pyFiles file:///tmp/src/udfs.py

-- // transactions
CREATE FUNCTION assign_operation AS 'udfs.assign_operation'
LANGUAGE PYTHON;

DROP TABLE IF EXISTS transactions_source;
CREATE TABLE transactions_source (
  `transaction_id`    STRING,
  `account_number`    INT,
  `customer_number`   INT,
  `event_timestamp`   TIMESTAMP(3),
  `type`              STRING,
  `amount`            INT
)
WITH (
  'connector' = 'faker', 
  'rows-per-second' = '1',
  'fields.transaction_id.expression' = '#{Internet.uuid}',
  'fields.account_number.expression' = '#{number.numberBetween ''0'',''10000''}',
  'fields.customer_number.expression' = '#{number.numberBetween ''0'',''5000''}',
  'fields.event_timestamp.expression' = '#{date.past ''15'',''2'',''SECONDS''}',
  'fields.type.expression' = '#{Options.option ''Credit'',''Debit'')}',
  'fields.amount.expression' = '#{number.numberBetween ''0'',''5000''}'
);

SELECT
  transaction_id,
  'A' || LPAD(CAST(account_number AS STRING), 8, '0') AS account_id,
  'C' || LPAD(CAST(customer_number AS STRING), 8, '0') AS customer_id,
   (1000 * EXTRACT(EPOCH FROM event_timestamp)) + EXTRACT(MILLISECOND FROM event_timestamp) AS event_time,
  event_timestamp,
  type,
  assign_operation(type) AS operation,
  amount
FROM transactions_source;


-- // customers
DROP TABLE IF EXISTS customers_source;
CREATE TABLE customers_source (
  `customer_number`   INT,
  `sex`               STRING,
  `dob`               DATE,
  `first_name`        STRING,
  `last_name`         STRING,
  `update_timestamp`  TIMESTAMP(3)
)
WITH (
  'connector' = 'faker', 
  'rows-per-second' = '1',
  'fields.customer_number.expression' = '#{number.numberBetween ''0'',''5000''}',
  'fields.sex.expression' = '#{Options.option ''Male'',''Female'')}',
  'fields.dob.expression' = '#{date.birthday}',
  'fields.first_name.expression' = '#{Name.firstName}',
  'fields.last_name.expression' = '#{Name.lastName}',
  'fields.update_timestamp.expression' = '#{date.past ''15'',''0'',''SECONDS''}'
);

SELECT
  'C' || LPAD(CAST(customer_number AS STRING), 8, '0') AS customer_id,
  sex,
  dob,
  first_name,
  last_name,
  LOWER(first_name) || '.' || LOWER(last_name) || '@email.com' AS email_address,
  (1000 * EXTRACT(EPOCH FROM update_timestamp)) + EXTRACT(MILLISECOND FROM update_timestamp) AS update_time,
  update_timestamp
FROM customers_source;


-- // accounts
DROP TABLE IF EXISTS accounts_source;
CREATE TABLE accounts_source (
  `account_number`    INT,
  `district_number`   INT,
  `frequency`         STRING,
  `update_timestamp`  TIMESTAMP(3)
)
WITH (
  'connector' = 'faker', 
  'rows-per-second' = '1',
  'fields.account_number.expression' = '#{number.numberBetween ''0'',''10000''}',
  'fields.district_number.expression' = '#{number.numberBetween ''0'',''100''}',
  'fields.frequency.expression' = '#{Options.option ''Weekly Issuance'',''Monthly Issuance'',''Issuance After Transaction'')}',
  'fields.update_timestamp.expression' = '#{date.past ''15'',''0'',''SECONDS''}'
);

SELECT
  'A' || LPAD(CAST(account_number AS STRING), 8, '0') AS account_id,
  CAST(district_number AS STRING) AS district_id,
  frequency,
  (1000 * EXTRACT(EPOCH FROM update_timestamp)) + EXTRACT(MILLISECOND FROM update_timestamp) AS update_time,
  update_timestamp
FROM accounts_source;
