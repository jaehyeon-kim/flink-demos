from pyflink.table import (
    EnvironmentSettings,
    TableEnvironment,
    CsvTableSource,
    CsvTableSink,
    WriteMode,
    DataTypes,
)

env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)
table_env.get_config().set("parallelism.default", "1")  # output to single file

field_names = "seller_id,product,quantity,product_price,sales_date".split(",")
field_types = [
    DataTypes.STRING(),
    DataTypes.STRING(),
    DataTypes.INT(),
    DataTypes.DOUBLE(),
    DataTypes.DATE(),
]

# source table
source = CsvTableSource("./csv-input", field_names, field_types, ignore_first_line=True)
table_env.register_table_source("product_locale_sales", source)
tbl = table_env.from_path("product_locale_sales")

# sink table
sink = CsvTableSink(
    field_names, field_types, "revenue.csv", num_files=1, write_mode=WriteMode.OVERWRITE
)
table_env.register_table_sink("locale_revenue", sink)
tbl.execute_insert("locale_revenue").wait()
