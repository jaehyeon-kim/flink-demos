from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, CsvTableSource

tbl_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())

field_names = "seller_id,product,quantity,product_price,sales_date".split(",")
field_types = [
    DataTypes.STRING(),
    DataTypes.STRING(),
    DataTypes.INT(),
    DataTypes.DOUBLE(),
    DataTypes.DATE(),
]
source = CsvTableSource("./csv-input", field_names, field_types, ignore_first_line=True)
tbl_env.register_table_source("product_locale_sales", source)
tbl = tbl_env.from_path("product_locale_sales")
print("\nProduct Sales Schema")
tbl.print_schema()
print("\nProduct Sales data")
print(tbl.to_pandas())
