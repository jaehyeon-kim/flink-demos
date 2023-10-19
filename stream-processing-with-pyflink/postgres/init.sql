CREATE SCHEMA demo;
GRANT ALL ON SCHEMA demo TO postgres;

-- change search_path on a connection-level
SET search_path TO demo;

-- change search_path on a database-level
ALTER database "main" SET search_path TO demo;

CREATE TABLE IF NOT EXISTS accounts_temp (
  accountId VARCHAR(50) PRIMARY KEY,
  districtId VARCHAR(10) NOT NULL,
  frequency VARCHAR (50) NOT NULL,
  parseDate VARCHAR(10) NOT NULL,
  year INT NOT NULL,
  month INT NOT NULL,
  day INT NOT NULL,
  dataDate VARCHAR(10) NOT NULL
);

COPY accounts_temp (accountId, districtId, frequency, parseDate, year, month, day, dataDate) 
  FROM '/tmp/accounts.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE accounts AS
    SELECT 
      accountId, 
      districtId, 
      frequency, 
      parseDate AS createDate, 
      CAST(EXTRACT (epoch FROM now()) AS BIGINT) AS updateTime
    FROM accounts_temp;