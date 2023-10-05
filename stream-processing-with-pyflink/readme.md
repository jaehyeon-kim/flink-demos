# Stream Processing with Pyflink

- [Stream Processing: Hands-on with Apache Flink](https://leanpub.com/streamprocessingwithapacheflink)
- [GitHub](https://github.com/polyzos/stream-processing-with-apache-flink)

## Streams and Tables

```bash
docker exec -it jobmanager ./bin/sql-client.sh
```

### Creating Tables

```sql
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'parallelism.default' = '5';
SET 'table.exec.source.idle-timeout' = '2000';

CREATE TABLE transactions (
  transactionId       STRING,
  accountId           STRING,
  customerId          STRING,
  eventTime           BIGINT,
  eventTime_ltz AS TO_TIMESTAMP_LTZ(eventTime, 3),
  eventTimeFormatted  STRING,
  type                STRING,
  operation           STRING,
  amount              DOUBLE,
  balance             DOUBLE,
  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
  WATERMARK FOR eventTime_ltz AS eventTime_ltz
) WITH (
  'connector' = 'kafka',
  'topic' = 'transactions',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'group.transactions',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);

SELECT
  transactionId,
  eventTime_ltz,
  type,
  amount,
  balance
FROM transactions;

CREATE TABLE customers (
  customerId STRING,
  sex         STRING,
  social      STRING,
  fullName    STRING,
  phone       STRING,
  email       STRING,
  address1    STRING,
  address2    STRING,
  city        STRING,
  state       STRING,
  zipcode     STRING,
  districtId  STRING,
  birthDate   STRING,
  updateTime  BIGINT,
  eventTime_ltz AS TO_TIMESTAMP_LTZ(updateTime, 3),
  WATERMARK FOR eventTime_ltz AS eventTime_ltz,
  PRIMARY KEY (customerId) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'customers',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'key.format' = 'json',
  'value.format' = 'json',
  'properties.group.id' = 'group.customers'
);

SELECT
  customerId,
  fullName,
  social,
  birthDate,
  updateTime
FROM customers;

CREATE TABLE accounts (
  accountId     STRING,
  districtId    STRING,
  frequency     STRING,
  creationDate  STRING,
  updateTime    BIGINT,
  eventTime_ltz AS TO_TIMESTAMP_LTZ(updateTime, 3),
  WATERMARK FOR eventTime_ltz AS eventTime_ltz,
  PRIMARY KEY (accountId) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'accounts',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'key.format' = 'json',
  'value.format' = 'json',
  'properties.group.id' = 'group.accounts'
);

SELECT * FROM accounts;
```

### Operators

```sql
-- stateless
SELECT
  transactionId,
  eventTime_ltz,
  type,
  amount,
  balance
FROM transactions
WHERE amount < 180000 AND type = 'Credit'
ORDER BY eventTime_ltz;

CREATE TEMPORARY VIEW temp_premium AS
SELECT
  transactionId,
  eventTime_ltz,
  type,
  amount,
  balance
FROM transactions
WHERE amount < 180000 AND type = 'Credit';

-- materialization
SET 'parallelism.default' = '5';

SELECT
  customerId,
  COUNT(transactionId) As txnCount
FROM transactions
GROUP BY customerId
LIMIT 10;

SELECT *
FROM (
  SELECT customerId, COUNT(transactionId) AS txnPerCustomer
  FROM transactions
  GROUP BY customerId
) AS e
WHERE txnPerCustomer > 1000;

SELECT customerId, COUNT(transactionId) AS txnPerCustomer
FROM transactions
GROUP BY customerId
HAVING COUNT(transactionId) > 1000;

-- temporal
SELECT
  transactionId,
  eventTime_ltz,
  convert_tz(
    CAST(eventTime_ltz AS string),
    'Australia/Sydney', 'UTC'
  ) AS eventTime_ltz_utc,
  type,
  amount,
  balance
FROM transactions
WHERE amount < 180000 AND type = 'Credit';

-- deduplication
SELECT transactionId, rowNum
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY transactionId ORDER BY eventTime_ltz) AS rowNum
    FROM transactions
)
WHERE rowNum = 1;
```

## Watermarks & Windows

```sql
-- // windows
SET sql-client.execution.result-mode = 'tableau';

CREATE TABLE trans (
  transactionId       STRING,
  accountId           STRING,
  customerId          STRING,
  dataTime            BIGINT,
  dataTime_ltz AS TO_TIMESTAMP_LTZ(dataTime, 3),
  dataTimeFormatted   STRING,
  type                STRING,
  operation           STRING,
  amount              DOUBLE,
  balance             DOUBLE,
  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
  WATERMARK FOR dataTime_ltz AS dataTime_ltz
) WITH (
  'connector' = 'kafka',
  'topic' = 'transactions',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'group.trans',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);

SELECT
  transactionId,
  dataTime_ltz
FROM trans;

-- tumbling window
SELECT
  window_start,
  window_end,
  COUNT(transactionId) AS txnCount
FROM TABLE (
  TUMBLE(TABLE trans, DESCRIPTOR(dataTime_ltz), INTERVAL '1' Day)
)
GROUP BY window_start, window_end;

-- sliding window
SELECT
  window_start,
  window_end,
  COUNT(transactionId) AS txnCount
FROM TABLE (
  HOP(TABLE trans, DESCRIPTOR(dataTime_ltz), INTERVAL '2' HOUR, INTERVAL '1' DAY)
)
GROUP BY window_start, window_end
LIMIT 20;

-- cumulative window
SELECT
  window_start,
  window_end,
  window_time,
  COUNT(transactionId) AS txnCount
FROM TABLE (
  CUMULATE(TABLE trans, DESCRIPTOR(dataTime_ltz), INTERVAL '2' HOUR, INTERVAL '1' DAY)
)
GROUP BY window_start, window_end, window_time
LIMIT 20;

-- session window
-- only supported in the DataStream API

-- Top-3 Customers per week (max # of transactions)
SELECT *
FROM (
  SELECT *,
    ROW_NUMBER() OVER(PARTITION BY window_start, window_end ORDER BY txnCount DESC) AS rowNum
  FROM (
    SELECT customerId, window_start, window_end, COUNT(transactionId) AS txnCount
    FROM TABLE (TUMBLE(TABLE trans, DESCRIPTOR(dataTime_ltz), INTERVAL '7' DAY))
    GROUP BY customerId, window_start, window_end
  )
)
WHERE rowNum <= 3;

-- // watermarks
SET sql-client.execution.result-mode = 'tableau';

CREATE TABLE small_trans (
  transactionId       STRING,
  accountId           STRING,
  customerId          STRING,
  dataTime            BIGINT,
  dataTime_ltz AS TO_TIMESTAMP_LTZ(dataTime, 3),
  dataTimeFormatted   STRING,
  type                STRING,
  operation           STRING,
  amount              DOUBLE,
  balance             DOUBLE,
  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
  WATERMARK FOR dataTime_ltz AS dataTime_ltz
) WITH (
  'connector' = 'kafka',
  'topic' = 'small-transactions',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'group.small-trans',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);

SELECT
  window_start,
  window_end,
  COUNT(transactionId) as txnCnt
FROM TABLE(
  TUMBLE(TABLE small_trans, DESCRIPTOR(dataTime_ltz), INTERVAL '7' DAY)
)
GROUP BY window_start, window_end;

set 'table.exec.source.idle-timeout' = '2000';
```

## Streaming Joins

```sql
SET sql-client.execution.result-mode = 'tableau';

CREATE TABLE transactions (
  transactionId       STRING,
  accountId           STRING,
  customerId          STRING,
  eventTime           BIGINT,
  eventTime_ltz AS TO_TIMESTAMP_LTZ(eventTime, 3),
  eventTimeFormatted  STRING,
  type                STRING,
  operation           STRING,
  amount              DOUBLE,
  balance             DOUBLE,
  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
  WATERMARK FOR eventTime_ltz AS eventTime_ltz
) WITH (
  'connector' = 'kafka',
  'topic' = 'transactions',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'group.transactions',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE customers (
  customerId STRING,
  sex         STRING,
  social      STRING,
  fullName    STRING,
  phone       STRING,
  email       STRING,
  address1    STRING,
  address2    STRING,
  city        STRING,
  state       STRING,
  zipcode     STRING,
  districtId  STRING,
  birthDate   STRING,
  updateTime  BIGINT,
  eventTime_ltz AS TO_TIMESTAMP_LTZ(updateTime, 3),
  WATERMARK FOR eventTime_ltz AS eventTime_ltz,
  PRIMARY KEY (customerId) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'customers',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'key.format' = 'json',
  'value.format' = 'json',
  'properties.group.id' = 'group.customers'
);

-- regular joins
-- records are ketp in state forever
-- or we can expire state via with table.exec.state.ttl
SELECT
  t.transactionId,
  t.eventTime_ltz,
  t.type,
  t.amount,
  t.balance,
  c.fullName,
  c.email,
  c.address1
FROM transactions AS t
JOIN customers AS c ON t.customerId = c.customerId;

-- interval joins
-- join records of two append-only tables such that
--    the time attributes of joined records are not more than a specified window interval apart
-- row are immediately removed from the state once they can no longer be joined
-- event-time skew between the streams can increase the state size
SELECT *
FROM A, B
WHERE A.id = B.id
  AND A.t BETWEEN B.t - INTERVAL '15' MINUTE AND B.t + INTERVAL '1' HOUR;

SELECT *
FROM A
JOIN B ON A.id = B.id
WHERE A.t BETWEEN B.t - INTERVAL '15' MINUTE AND B.t + INTERVAL '1' HOUR;

CREATE TABLE debits (
  transactionId       STRING,
  accountId           STRING,
  customerId          STRING,
  dataTime           BIGINT,
  dataTime_ltz AS TO_TIMESTAMP_LTZ(dataTime, 3),
  dataTimeFormatted  STRING,
  type                STRING,
  operation           STRING,
  amount              DOUBLE,
  balance             DOUBLE,
  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
  WATERMARK FOR dataTime_ltz AS dataTime_ltz
) WITH (
  'connector' = 'kafka',
  'topic' = 'transactions.debits',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'group.transactions.debits',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE credits (
  transactionId       STRING,
  accountId           STRING,
  customerId          STRING,
  dataTime           BIGINT,
  dataTime_ltz AS TO_TIMESTAMP_LTZ(dataTime, 3),
  dataTimeFormatted  STRING,
  type                STRING,
  operation           STRING,
  amount              DOUBLE,
  balance             DOUBLE,
  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
  WATERMARK FOR dataTime_ltz AS dataTime_ltz
) WITH (
  'connector' = 'kafka',
  'topic' = 'transactions.credits',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'group.transactions.credits',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);

SELECT
  d.transactionId AS debitId,
  c.customerId AS debitCid,
  d.dataTime_ltz AS debitDataTime,
  c.transactionId As creditId,
  c.customerId AS creditCid,
  c.dataTime_ltz AS creditDataTime
FROM debits d
JOIN credits c ON d.customerId = c.customerId
WHERE d.dataTime_ltz BETWEEN c.dataTime_ltz - INTERVAL '1' HOUR AND c.dataTime_ltz;

-- temporal joins
-- Temporal table joins take an arbitrary table (left input/probe site) and correlate each row to the corresponding row's relevant version in a versioned table (right input/build side).
SELECT
  t.transactionId,
  t.eventTime_ltz,
  TO_TIMESTAMP_LTZ(c.updateTime, 3) AS updateTime,
  t.type,
  t.amount,
  t.balance,
  c.fullName,
  c.email,
  c.address1
FROM transactions AS t
JOIN customers FOR SYSTEM_TIME AS OF t.eventTime_ltz AS c
ON t.customerId = c.customerId;

SET 'parallelism.default' = '5';
set 'table.exec.source.idle-timeout' = '2000';

-- lookup joins
-- better suited for enriching dynamic tables with data from an external table
-- JDBC supports an optional lookup cache (disabled by default)
--    and you can configure it using lookup.cache.max-rows and lookup.cache.ttl
CREATE TABLE accounts (
  accountId STRING,
  districtId STRING,
  frequency STRING,
  createDate STRING,
  updateTime BIGINT,
  eventTime_ltz AS TO_TIMESTAMP_LTZ(updateTime, 3),
  WATERMARK FOR eventTime_ltz AS eventTime_ltz,
  PRIMARY KEY (accountId) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/main',
  'table-name' = 'accounts',
  'username' = 'postgres',
  'password' = 'postgres'
);

SELECT
  t.transactionId,
  t.accountId,
  t.eventTime_ltz,
  TO_TIMESTAMP_LTZ(a.updateTime, 3) AS updateTime,
  t.type,
  t.amount,
  t.balance,
  a.districtId,
  a.frequency
FROM transactions AS t
JOIN accounts FOR SYSTEM_TIME AS OF t.eventTime_ltz AS a
ON t.accountId = a.accountId;
```

## User Defined Functions

```sql
SET sql-client.execution.result-mode = 'tableau';

CREATE FUNCTION async_lookup AS 'python_udf.async_lookup'
LANGUAGE PYTHON;

CREATE TABLE small_trans (
  transactionId       STRING,
  accountId           STRING,
  customerId          STRING,
  dataTime            BIGINT,
  dataTime_ltz AS TO_TIMESTAMP_LTZ(dataTime, 3),
  dataTimeFormatted   STRING,
  type                STRING,
  operation           STRING,
  amount              DOUBLE,
  balance             DOUBLE,
  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
  WATERMARK FOR dataTime_ltz AS dataTime_ltz
) WITH (
  'connector' = 'kafka',
  'topic' = 'small-transactions',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'group.small-trans',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE trans_resp(
  transactionId       STRING,
  resp Row<`resp_1` STRING, `resp_2` STRING, `resp_3` STRING, `total_resp_time` FLOAT>
) WITH (
  'connector' = 'kafka',
  'topic' = 'trans-resp',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'group.trans-resp',
  'format' = 'json'
);

INSERT INTO trans_resp
  SELECT transactionId, async_lookup(transactionId) AS resp FROM small_trans;

SELECT
  transactionId,
  async_lookup(transactionId) AS resp
FROM small_trans
LIMIT 5;
```
