## Flink SQL Cookbook on Docker

A Flink cluster that can be used to run queries of the [Apache Flink SQL Cookbook](https://github.com/ververica/flink-sql-cookbook/tree/main) repo from Ververica.

The Flink Docker image is updated with the [Flink SQL Faker Connector](https://github.com/knaufk/flink-faker) for fake data generation. Note that the example SQL queries are based on an old version of the connector, and some of them have to be modified.

### Flink Cluster on Docker

The cookbook generates sample records using the [Flink SQL Faker Connector](https://github.com/knaufk/flink-faker), and we use a custom Docker image that downloads its source to the `/opt/flink/lib/` folder. In this way, we don't have to specify the connector source whenever we start the [SQL client](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sqlclient/).

```Dockerfile
FROM flink:1.20.1

# add faker connector
RUN wget -P /opt/flink/lib/ \
  https://github.com/knaufk/flink-faker/releases/download/v0.5.3/flink-faker-0.5.3.jar
```

A local Apache Flink cluster can be deployed using Docker Compose.

```bash
# start containers
$ docker compose up -d

# list containers
$ docker-compose ps
# NAME                COMMAND                  SERVICE             STATUS              PORTS
# jobmanager          "/docker-entrypoint.…"   jobmanager          running (healthy)   6123/tcp, 0.0.0.0:8081->8081/tcp, :::8081->8081/tcp
# taskmanager-1       "/docker-entrypoint.…"   taskmanager-1       running             6123/tcp, 8081/tcp
# taskmanager-2       "/docker-entrypoint.…"   taskmanager-2       running             6123/tcp, 8081/tcp
# taskmanager-3       "/docker-entrypoint.…"   taskmanager-3       running             6123/tcp, 8081/tcp
```

### Flink SQL Client

```sql
-- // create a temporary table
CREATE TEMPORARY TABLE heros (
  `name` STRING,
  `power` STRING,
  `age` INT
) WITH (
  'connector' = 'faker',
  'fields.name.expression' = '#{superhero.name}',
  'fields.power.expression' = '#{superhero.power}',
  'fields.power.null-rate' = '0.05',
  'fields.age.expression' = '#{number.numberBetween ''0'',''1000''}'
);
-- [INFO] Execute statement succeeded.

-- list tables
SHOW TABLES;
-- +------------+
-- | table name |
-- +------------+
-- |      heros |
-- +------------+
-- 1 row in set

-- query records from the heros table
-- hit 'q' to exit the record view
SELECT * FROM heros;

-- quit sql shell
quit;
```

![](./img/sql-client.gif#center)
