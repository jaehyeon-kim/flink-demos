from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, TableDescriptor, Schema
from pyflink.table.expressions import col
from pyflink.table.udf import udf
import pandas as pd

###
### Intro to the Python Table API
###
### Common Structure of Python Table API Program
# 1. create a TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# 2. create source table
table_env.execute_sql(
    """
    CREATE TABLE datagen (
        id INT,
        data STRING
    ) WITH (
        'connector' = 'datagen',
        'fields.id.kind' = 'sequence',
        'fields.id.start' = '1',
        'fields.id.end' = '10'
    )
"""
)

# 3. create sink table
table_env.execute_sql(
    """
    CREATE TABLE print (
        id INT,
        data STRING
    ) WITH (
        'connector' = 'print'
    )
"""
)

# 4. query from source table and perform calculations
# create a table from a table api query:
source_table = table_env.from_path("datagen")
# or create a table from a sql query
# source_table = table_env.sql_query("SELECT * from datagen")

result_table = source_table.select(col("id") + 1, col("data"))

# 5. emit query result to sink table
# emit a table api result table to a sink table
result_table.execute_insert("print").wait()
# or emit result via sql query:
# table_env.execute_sql("INSERT INTO print SELECT * FROM datagen").wait()

### Create Tables
# 1. create using a list object
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

elems = [(1, "Hi"), (2, "Hello")]
col_names = ["id", "data"]
schema = DataTypes.ROW(
    [DataTypes.FIELD("id", DataTypes.TINYINT()), DataTypes.FIELD("data", DataTypes.STRING())]
)

table = table_env.from_elements(elems)
table.execute().print()

table = table_env.from_elements(elems, col_names)
print(f"By default, the type of the `id` column is {table.get_schema().get_field_data_type('id')}")
# BIGINT

table = table_env.from_elements(elems, schema)
print(f"By default, the type of the `id` column is {table.get_schema().get_field_data_type('id')}")
# TINYINT

# 2. create using DDL statements
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table_env.execute_sql(
    """
    CREATE TABLE random_source (
        id BiGINT,
        data TINYINT
    ) WITH (
        'connector' = 'datagen',
        'fields.id.kind' = 'sequence',
        'fields.id.start' = '1',
        'fields.id.end' = '3',
        'fields.data.kind' = 'sequence',
        'fields.data.start' = '4',
        'fields.data.end' = '6'
    )
"""
)
table = table_env.from_path("random_source")
table.execute().print()

# 3. create using TableDescriptor
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table_env.create_temporary_table(
    "random_source",
    TableDescriptor.for_connector("datagen")
    .schema(
        Schema.new_builder()
        .column("id", DataTypes.BIGINT())
        .column("data", DataTypes.TINYINT())
        .build()
    )
    .option("fields.id.kind", "sequence")
    .option("fields.id.start", "1")
    .option("fields.id.end", "3")
    .option("fields.data.kind", "sequence")
    .option("fields.data.start", "4")
    .option("fields.data.end", "6")
    .build(),
)
table = table_env.from_path("random_source")
table.execute().print()

# 4. create using a catalog
# prepare the catalog by registering Table API tables in the catalog
table = table_env.from_elements([(1, "Hi"), (2, "Hello")], ["id", "data"])
table_env.create_temporary_view("source_table", table)

# create Table API table from catalog
new_table = table_env.from_path("source_table")
new_table.execute().print()

### Write Queries
# 1. write table api queries
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

orders = table_env.from_elements(
    [("Jack", "FRANCE", 10), ("Rose", "ENGLAND", 30), ("Jack", "FRANCE", 20)],
    ["name", "country", "revenue"],
)

revenue = (
    orders.select(col("name"), col("country"), col("revenue"))
    .where(col("country") == "FRANCE")
    .group_by(col("name"))
    .select(col("name"), col("revenue").sum.alias("rev_sum"))
)

revenue.execute().print()

# row-based operation
map_function = udf(
    lambda x: pd.concat([x.name, x.revenue * 10], axis=1),
    result_type=DataTypes.ROW(
        [
            DataTypes.FIELD("name", DataTypes.STRING()),
            DataTypes.FIELD("revenue", DataTypes.BIGINT()),
        ]
    ),
    func_type="pandas",
)
orders.map(map_function).execute().print()

# 2. write sql queries
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table_env.execute_sql(
    """
    CREATE TABLE random_source (
        id BIGINT, 
        data TINYINT
    ) WITH (
        'connector' = 'datagen',
        'fields.id.kind'='sequence',
        'fields.id.start'='1',
        'fields.id.end'='8',
        'fields.data.kind'='sequence',
        'fields.data.start'='4',
        'fields.data.end'='11'
    )
"""
)

table_env.execute_sql(
    """
    CREATE TABLE print_sink (
        id BIGINT, 
        data_sum TINYINT 
    ) WITH (
        'connector' = 'print'
    )
"""
)

table_env.execute_sql(
    """
    INSERT INTO print_sink
        SELECT id, sum(data) AS data_sum
        FROM (SELECT id / 2 AS id, data FROM random_source)
        WHERE id > 1
        GROUP BY id
"""
).wait()

# 3. mix table api and sql
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# table object in sql
table = table_env.from_elements([(1, "Hi"), (2, "Hello")], ["id", "data"])
table_env.create_temporary_view("table_api_table", table)

table_env.execute_sql(
    """
    CREATE TABLE table_sink (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'print'
    )
"""
)

table_env.execute_sql("INSERT INTO table_sink SELECT * FROM table_api_table").wait()

# sql table in table api
table_env.execute_sql(
    """
    CREATE TABLE sql_source (
        id BIGINT, 
        data TINYINT 
    ) WITH (
        'connector' = 'datagen',
        'fields.id.kind'='sequence',
        'fields.id.start'='1',
        'fields.id.end'='4',
        'fields.data.kind'='sequence',
        'fields.data.start'='4',
        'fields.data.end'='7'
    )
"""
)

table = table_env.from_path("sql_source")
table.execute().print()

#### Emit Results
# print table
source = table_env.from_elements([(1, "Hi", "Hello"), (2, "Hello", "Hello")], ["a", "b", "c"])
table_result = table_env.execute_sql("select a + 1, b, c from %s" % source)

table_result.print()

# collect results to client
source = table_env.from_elements([(1, "Hi", "Hello"), (2, "Hello", "Hello")], ["a", "b", "c"])
table_result = table_env.execute_sql("select a + 1, b, c from %s" % source)

with table_result.collect() as results:
    for result in results:
        print(result)

# collect as pandas
source = table_env.from_elements([(1, "Hi", "Hello"), (2, "Hello", "Hello")], ["a", "b", "c"])
print(source.to_pandas())

# emit to one sink table
table_env.execute_sql(
    """
    CREATE TABLE sink_table (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'print'
    )
"""
)

table = table_env.from_elements([(1, "Hi"), (2, "Hello")], ["id", "data"])
table.execute_insert("sink_table").wait()

# via sql
table_env.create_temporary_view("table_source", table)
table_env.execute_sql("INSERT INTO sink_table SELECT * FROM table_source").wait()

# multiple sink tables
table = table_env.from_elements([(1, "Hi"), (2, "Hello")], ["id", "data"])
table_env.create_temporary_view("simple_source", table)

table_env.execute_sql(
    """
    CREATE TABLE first_sink_table (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'print'
    )
"""
)
table_env.execute_sql(
    """
    CREATE TABLE second_sink_table (
        id BIGINT, 
        data VARCHAR
    ) WITH (
        'connector' = 'print'
    )
"""
)

statement_set = table_env.create_statement_set()
statement_set.add_insert("first_sink_table", table)
statement_set.add_insert_sql("INSERT INTO second_sink_table SELECT * FROM simple_source")
statement_set.execute().wait()

#### Explain Tables
## table
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table1 = table_env.from_elements([(1, "Hi"), (2, "Hello")], ["id", "data"])
table2 = table_env.from_elements([(1, "Hi"), (2, "Hello")], ["id", "data"])
table = table1.where(col("data").like("H%")).union_all(table2)
print(table.explain())

## statement set
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table1 = table_env.from_elements([(1, "Hi"), (2, "Hello")], ["id", "data"])
table2 = table_env.from_elements([(1, "Hi"), (2, "Hello")], ["id", "data"])
table_env.execute_sql(
    """
    CREATE TABLE print_sink_table (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'print'
    )
"""
)
table_env.execute_sql(
    """
    CREATE TABLE black_hole_sink_table (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'blackhole'
    )
"""
)

statement_set = table_env.create_statement_set()

statement_set.add_insert("print_sink_table", table1.where(col("data").like("H%")))
statement_set.add_insert("black_hole_sink_table", table2)

print(statement_set.explain())
