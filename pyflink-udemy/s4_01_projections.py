from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, CsvTableSource
from pyflink.table.expressions import col

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

redundant_prices = tbl.select(col("product"), col("product_price").alias("price"))
print("\nredundant_prices data")
redundant_prices.execute().print()

redundant_prices2 = tbl_env.sql_query(
    f"SELECT product, product_price As price FROM product_locale_sales"
)
print("\nredundant_prices2 data")
redundant_prices.execute().print()

distinct_prices = tbl.select(col("product"), col("product_price").alias("price")).distinct()
print("\ndistinct_prices data")
distinct_prices.execute().print()

distinct_prices2 = tbl_env.sql_query(
    "SELECT DISTINCT product, product_price AS price FROM product_locale_sales"
)
print("\ndistinct_prices2 data")
distinct_prices2.execute().print()

product_sales = tbl.select(
    col("sales_date"),
    col("seller_id"),
    col("product"),
    (col("product_price") * col("quantity")).alias("sales"),
).distinct()
print("\nproduct_sales data")
product_sales.execute().print()

product_sales2 = tbl_env.sql_query(
    """
    SELECT DISTINCT
        sales_date, seller_id, product, product_price * quantity AS sales
    FROM product_locale_sales
    """
)
print("\nproduct_sales2 data")
product_sales2.execute().print()
