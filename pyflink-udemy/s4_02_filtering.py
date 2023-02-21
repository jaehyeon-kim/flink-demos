from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, CsvTableSource
from pyflink.table.expressions import col, lit, and_

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

high_sales = (
    tbl.select(
        col("sales_date"),
        col("seller_id"),
        col("product"),
        (col("product_price") * col("quantity")).alias("sales"),
    )
    .distinct()
    .where(col("sales") >= 80)
)
print("\nhigh_sales data")
high_sales.execute().print()

high_sales2 = tbl_env.sql_query(
    """
    WITH distinct_sales AS (
        SELECT DISTINCT
            sales_date, seller_id, product, product_price * quantity AS sales
        FROM product_locale_sales
    )
    SELECT *
    FROM distinct_sales
    WHERE sales >= 80
    """
)
print("\nhigh_sales2 data")
high_sales2.execute().print()

july1_high_sales = (
    tbl.select(
        col("sales_date"),
        col("seller_id"),
        col("product"),
        (col("product_price") * col("quantity")).alias("sales"),
    )
    .distinct()
    .where(and_(col("sales") >= 80, col("sales_date") == lit("2021-07-01").to_date))
)
print("\njuly1_high_sales data")
july1_high_sales.execute().print()

july1_high_sales2 = tbl_env.sql_query(
    """
    WITH distinct_sales AS (
        SELECT DISTINCT
            sales_date, seller_id, product, product_price * quantity AS sales
        FROM product_locale_sales
    )
    SELECT *
    FROM distinct_sales
    WHERE sales >= 80 and sales_date = '2021-07-01'    
    """
)
print("\njuly1_high_sales2 data")
july1_high_sales2.execute().print()
