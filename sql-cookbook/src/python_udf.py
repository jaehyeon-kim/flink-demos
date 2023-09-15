from pyflink.table import DataTypes
from pyflink.table.udf import udf

us_cities = {"Chicago", "Portland", "Seattle", "New York"}


@udf(input_types=[DataTypes.STRING(), DataTypes.FLOAT()], result_type=DataTypes.FLOAT())
def to_fahr(city, temperature):
    return temperature if city not in us_cities else (temperature * 9.0 / 5.0) + 32.0
