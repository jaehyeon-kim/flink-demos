from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, CsvTableSource
from pyflink.table.expressions import col

tbl_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())

# sales source
sales_field_names = "seller_id,product,quantity,product_price,sales_date".split(",")
sales_field_types = [
    DataTypes.STRING(),
    DataTypes.STRING(),
    DataTypes.INT(),
    DataTypes.DOUBLE(),
    DataTypes.DATE(),
]
sales_source = CsvTableSource(
    "./csv-input", sales_field_names, sales_field_types, ignore_first_line=True
)
tbl_env.register_table_source("product_locale_sales", sales_source)

# sellers source
sellers_field_names = "id,city,state".split(",")
sellers_field_types = [DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()]
sellers_source = CsvTableSource(
    "./seller-input", sellers_field_names, sellers_field_types, ignore_first_line=True
)
tbl_env.register_table_source("seller_locales", sellers_source)

sales_tbl = tbl_env.from_path("product_locale_sales")
sellers_tbl = tbl_env.from_path("seller_locales")

seller_products = (
    sales_tbl.join(sellers_tbl, col("seller_id") == col("id"))
    .select(col("city"), col("state"), col("product"), col("product_price"))
    .distinct()
)
print("\nseller_products data")
seller_products.execute().print()

seller_products2 = tbl_env.sql_query(
    """
    SELECT DISTINCT city, state, product, product_price
    FROM product_locale_sales l
    JOIN seller_locales r ON l.seller_id = r.id
    """
)
print("\nseller_products2 data")
seller_products2.execute().print()

sellers_no_sales = (
    sales_tbl.right_outer_join(sellers_tbl, col("seller_id") == col("id"))
    .where(col("product").is_null)
    .select(col("city"), col("state"), col("product"))
    .distinct()
)
print("\nsellers_no_sales data")
sellers_no_sales.execute().print()

sellers_no_sales2 = tbl_env.sql_query(
    """
    SELECT city, state, product
    FROM product_locale_sales l
    RIGHT JOIN seller_locales r ON l.seller_id = r.id
    WHERE product IS NULL
    """
)
print("\nsellers_no_sales2 data")
sellers_no_sales2.execute().print()
