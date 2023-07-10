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

tbl.order_by(col("quantity").asc).execute().print()
tbl.order_by(col("quantity").asc).offset(1).fetch(2).execute().print()
tbl.order_by(col("quantity").asc).fetch(8).execute().print()

avg_price = (
    tbl.select(col("product_price")).distinct().select(col("product_price").avg.alias("avg_price"))
)
print("\navg_price data")
avg_price.execute().print()

avg_price2 = tbl_env.sql_query(
    """
    SELECT avg(product_price) AS avg_price
    FROM product_locale_sales
    """
)
print("\navg_price2 data")
avg_price2.execute().print()

seller_revenue = (
    tbl.select(
        col("seller_id"), col("product"), (col("product_price") * col("quantity")).alias("sales")
    )
    .group_by(col("seller_id"))
    .select(col("seller_id"), col("sales").sum.alias("seller_revenue"))
)
print("\nseller_revenue data")
seller_revenue.execute().print()

seller_revenue2 = tbl_env.sql_query(
    """
    SELECT seller_id, sum(product_price * quantity) AS seller_revenue
    FROM product_locale_sales
    GROUP BY seller_id
    """
)
print("\nseller_revenue2 data")
seller_revenue2.execute().print()
