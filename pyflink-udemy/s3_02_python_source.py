from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.types import DataTypes

tbl_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())

products = [("Toothbrush", 3.99), ("Dental Floss", 1.99), ("Toothpaste", 4.99)]

## tbl1
tbl1 = tbl_env.from_elements(products)
print("\ntbl1 schema")
tbl1.print_schema()
print("\ntbl1 data")
print(tbl1.to_pandas())

## tbl2
col_names = ["product", "price"]
tbl2 = tbl_env.from_elements(products, col_names)
print("\ntbl2 schema")
tbl2.print_schema()
print("\ntbl2 data")
print(tbl2.to_pandas())

## tbl3
schema = DataTypes.ROW(
    [DataTypes.FIELD("product", DataTypes.STRING()), DataTypes.FIELD("price", DataTypes.DOUBLE())]
)
tbl3 = tbl_env.from_elements(products, schema)
print("\ntbl3 schema")
tbl3.print_schema()
print("\ntbl3 data")
print(tbl3.to_pandas())
