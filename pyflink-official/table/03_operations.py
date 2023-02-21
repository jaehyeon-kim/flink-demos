import json

from pyflink.table import TableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.expressions import *
from pyflink.table.udf import udtf, udf, udaf, udtaf, AggregateFunction, TableAggregateFunction
from pyflink.common import Row

# https://github.com/apache/flink/blob/release-1.16/flink-python/pyflink/examples/table/basic_operations.py

####
#### Basic operations
####
t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

table = t_env.from_elements(
    elements=[
        (1, '{"name": "Flink", "tel": 123, "addr": {"country": "Germany", "city": "Berlin"}}'),
        (2, '{"name": "hello", "tel": 135, "addr": {"country": "China", "city": "Shanghai"}}'),
        (3, '{"name": "world", "tel": 124, "addr": {"country": "USA", "city": "NewYork"}}'),
        (4, '{"name": "PyFlink", "tel": 32, "addr": {"country": "China", "city": "Hangzhou"}}'),
    ],
    schema=["id", "data"],
)

right_table = t_env.from_elements(
    elements=[(1, 18), (2, 30), (3, 25), (4, 10)], schema=["id", "age"]
)

table = table.add_columns(
    col("data").json_value("$.name", DataTypes.STRING()).alias("name"),
    col("data").json_value("$.tel", DataTypes.STRING()).alias("tel"),
    col("data").json_value("$.addr.country", DataTypes.STRING()).alias("country"),
).drop_columns(col("data"))

table.execute().print()
# limit the number of outputs
table.limit(3).execute().print()
# filter
table.filter(col("id") != 3).execute().print()
# aggregation
table.group_by(col("country")).select(
    col("country"), col("id").count, col("tel").cast(DataTypes.BIGINT()).max
).execute().print()
# distinct
table.select(col("country")).distinct().execute().print()
# join
# note that it still doesn't support duplicate column names between the joined tables
table.join(
    right_table.rename_columns(col("id").alias("r_id")), col("id") == col("r_id")
).execute().print()


# join literal
@udtf(result_types=[DataTypes.STRING()])
def split(r: Row):
    for s in r.name.split("i"):
        yield s


table.join_lateral(split.alias("a")).execute().print()

# show schema
table.print_schema()
# show execute plan
print(table.join_lateral(split.alias("a")).explain())

####
#### SQL operations
####
t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
table = t_env.from_elements(
    elements=[
        (1, '{"name": "Flink", "tel": 123, "addr": {"country": "Germany", "city": "Berlin"}}'),
        (2, '{"name": "hello", "tel": 135, "addr": {"country": "China", "city": "Shanghai"}}'),
        (3, '{"name": "world", "tel": 124, "addr": {"country": "USA", "city": "NewYork"}}'),
        (4, '{"name": "PyFlink", "tel": 32, "addr": {"country": "China", "city": "Hangzhou"}}'),
    ],
    schema=["id", "data"],
)
t_env.sql_query(f"SELECT * FROM {table}").execute().print()


# execute sql statement
@udtf(result_types=[DataTypes.STRING(), DataTypes.INT(), DataTypes.STRING()])
def parse_data(data: str):
    json_data = json.loads(data)
    yield json_data["name"], json_data["tel"], json_data["addr"]["country"]


t_env.create_temporary_function("parse_data", parse_data)
t_env.execute_sql(
    """
    SELECT *
    FROM %s, LATERAL TABLE(parse_data(`data`)) t(name, tel, country)
    """
    % table
).print()

# explain sql plan
print(
    t_env.explain_sql(
        """
    SELECT *
    FROM %s, LATERAL TABLE(parse_data(`data`)) t(name, tel, country)
    """
        % table
    )
)

####
#### Column operations
####
t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

table = t_env.from_elements(
    elements=[
        (1, '{"name": "Flink", "tel": 123, "addr": {"country": "Germany", "city": "Berlin"}}'),
        (2, '{"name": "hello", "tel": 135, "addr": {"country": "China", "city": "Shanghai"}}'),
        (3, '{"name": "world", "tel": 124, "addr": {"country": "USA", "city": "NewYork"}}'),
        (4, '{"name": "PyFlink", "tel": 32, "addr": {"country": "China", "city": "Hangzhou"}}'),
    ],
    schema=["id", "data"],
)

table = table.add_columns(
    col("data").json_value("$.name", DataTypes.STRING()).alias("name"),
    col("data").json_value("$.tel", DataTypes.STRING()).alias("tel"),
    col("data").json_value("$.addr.country", DataTypes.STRING()).alias("country"),
)

table.execute().print()

# drop columns
table = table.drop_columns(col("data"))
table.execute().print()

# replace columns
table = table.add_or_replace_columns(
    concat(col("id").cast(DataTypes.STRING()), "_", col("name")).alias("id")
)
table.execute().print()

####
#### Row operations
####
t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

table = t_env.from_elements(
    elements=[
        (1, '{"name": "Flink", "tel": 123, "addr": {"country": "Germany", "city": "Berlin"}}'),
        (2, '{"name": "hello", "tel": 135, "addr": {"country": "China", "city": "Shanghai"}}'),
        (3, '{"name": "world", "tel": 124, "addr": {"country": "China", "city": "NewYork"}}'),
        (4, '{"name": "PyFlink", "tel": 32, "addr": {"country": "China", "city": "Hangzhou"}}'),
    ],
    schema=["id", "data"],
)


# map operation
@udf(
    result_type=DataTypes.ROW(
        [DataTypes.FIELD("id", DataTypes.BIGINT()), DataTypes.FIELD("country", DataTypes.STRING())]
    )
)
def extract_country(input_row: Row):
    data = json.loads(input_row.data)
    return Row(input_row.id, data["addr"]["country"])


table.map(extract_country).execute().print()


# flat map operation
@udtf(result_types=[DataTypes.BIGINT(), DataTypes.STRING()])
def extract_city(input_row: Row):
    data = json.loads(input_row.data)
    return input_row.id, data["addr"]["city"]


table.flat_map(extract_city).select(
    col("f0").alias("id"), col("f1").alias("city")
).execute().print()


# aggregate operation
class CountAndSumAggregateFunction(AggregateFunction):
    def get_value(self, accumulator):
        return Row(accumulator[0], accumulator[1])

    def create_accumulator(self):
        return Row(0, 0)

    def accumulate(self, accumulator, input_row):
        accumulator[0] += 1
        accumulator[1] += int(input_row.tel)

    def retract(self, accumulator, input_row):
        accumulator[0] += 1
        accumulator[1] -= int(input_row.tel)

    def merge(self, accumulator, accumulators):
        for other_acc in accumulators:
            accumulator[0] += other_acc[0]
            accumulator[1] += other_acc[1]

    def get_accumulator_type(self):
        return DataTypes.ROW(
            [DataTypes.FIELD("cnt", DataTypes.BIGINT()), DataTypes.FIELD("sum", DataTypes.BIGINT())]
        )

    def get_result_type(self):
        return DataTypes.ROW(
            [DataTypes.FIELD("cnt", DataTypes.BIGINT()), DataTypes.FIELD("sum", DataTypes.BIGINT())]
        )


count_sum = udaf(CountAndSumAggregateFunction())
table.add_columns(
    col("data").json_value("$.name", DataTypes.STRING()).alias("name"),
    col("data").json_value("$.tel", DataTypes.STRING()).alias("tel"),
    col("data").json_value("$.addr.country", DataTypes.STRING()).alias("country"),
).group_by(col("country")).aggregate(count_sum.alias("cnt", "sum")).select(
    col("country"), col("cnt"), col("sum")
).execute().print()


# flat agregate operation
class Top2(TableAggregateFunction):
    def emit_value(self, accumulator):
        for v in accumulator:
            if v:
                yield Row(v)

    def create_accumulator(self):
        return [None, None]

    def accumulate(self, accumulator, input_row):
        tel = int(input_row.tel)
        if accumulator[0] is None or tel > accumulator[0]:
            accumulator[1] = accumulator[0]
            accumulator[0] = tel
        elif accumulator[1] is None or tel > accumulator[1]:
            accumulator[1] = tel

    def get_accumulator_type(self):
        return DataTypes.ARRAY(DataTypes.BIGINT())

    def get_result_type(self):
        return DataTypes.ROW([DataTypes.FIELD("tel", DataTypes.BIGINT())])


top2 = udtaf(Top2())
table.add_columns(
    col("data").json_value("$.name", DataTypes.STRING()).alias("name"),
    col("data").json_value("$.tel", DataTypes.STRING()).alias("tel"),
    col("data").json_value("$.addr.country", DataTypes.STRING()).alias("country"),
).group_by(col("country")).flat_aggregate(top2).select(col("country"), col("tel")).execute().print()
