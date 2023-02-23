from statistics import stdev, mean

from pyflink.common import Row
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, CsvTableSource
from pyflink.table.udf import udf

tbl_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())

field_names = "seller_id,q1,q2,q3,q4".split(",")
field_types = [
    DataTypes.STRING(),
    DataTypes.INT(),
    DataTypes.INT(),
    DataTypes.INT(),
    DataTypes.INT(),
]
source = CsvTableSource("./quarterly-sales-input", field_names, field_types, ignore_first_line=True)
tbl_env.register_table_source("quarterly_sales", source)

tbl = tbl_env.from_path("quarterly_sales")
print("\nQuarterly Sales Schema")
tbl.print_schema()

print("\nQuarterly Sales Data")
tbl.execute().print()


@udf(
    result_type=DataTypes.ROW(
        [
            DataTypes.FIELD("seller_id", DataTypes.STRING()),
            DataTypes.FIELD("sales_total", DataTypes.INT()),
            DataTypes.FIELD("qtr_avg", DataTypes.DOUBLE()),
            DataTypes.FIELD("qtr_stdev", DataTypes.DOUBLE()),
        ]
    )
)
def sales_summary_stats(seller_sales: Row) -> Row:
    seller_id, q1, q2, q3, q4 = seller_sales
    sales = (q1, q2, q3, q4)
    total_sales = sum(sales)
    qtr_avg = round(mean(sales), 2)
    qtr_stdev = round(stdev(sales), 2)
    return Row(seller_id, total_sales, qtr_avg, qtr_stdev)


sales_stats = tbl.map(sales_summary_stats).alias(
    "seller_id", "total_sales", "quarterly_avg", "quarterly_stdev"
)

print("\nSales Summary Stats schema")
sales_stats.print_schema()

print("\nSales Summary Stats data")
sales_stats.execute().print()
