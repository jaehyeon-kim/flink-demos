import argparse
import logging
import sys

from pyflink.common import Row
from pyflink.table import (
    EnvironmentSettings,
    TableEnvironment,
    TableDescriptor,
    DataTypes,
    FormatDescriptor,
    Schema,
)

from pyflink.table.expressions import lit, col
from pyflink.table.udf import udtf

from data import word_count_data


def create_input_table(t_env: TableEnvironment, input_path):
    if input_path is not None:
        t_env.create_temporary_table(
            "source",
            TableDescriptor.for_connector("filesystem")
            .schema(Schema.new_builder().column("word", DataTypes.STRING()).build())
            .option("path", input_path)
            .format("csv")
            .build(),
        )
        return t_env.from_path("source")
    else:
        print("Executing word_count example with default input data set.")
        print("Use --input to specify file input.")
        return t_env.from_elements(
            map(lambda i: (i,), word_count_data),
            DataTypes.ROW([DataTypes.FIELD("line", DataTypes.STRING())]),
        )


def create_output_table(t_env: TableEnvironment, output_path):
    if output_path is not None:
        t_env.create_temporary_table(
            "sink",
            TableDescriptor.for_connector("filesystem")
            .schema(
                Schema.new_builder()
                .column("word", DataTypes.STRING())
                .column("count", DataTypes.BIGINT())
                .build()
            )
            .option("path", output_path)
            .format(FormatDescriptor.for_format("canal-json").build())
            .build(),
        )
    else:
        print("Printing result to stdout. Use --output to specify output path.")
        t_env.create_temporary_table(
            "sink",
            TableDescriptor.for_connector("print")
            .schema(
                Schema.new_builder()
                .column("word", DataTypes.STRING())
                .column("count", DataTypes.BIGINT())
                .build()
            )
            .build(),
        )


def word_count(input_path, output_path):
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    t_env.get_config().set("parallelism.default", "1")

    tab = create_input_table(t_env, input_path)
    create_output_table(t_env, output_path)

    @udtf(result_types=[DataTypes.STRING()])
    def split(line: Row):
        for s in line[0].split():
            yield Row(s)

    tab.flat_map(split).alias("word").group_by(col("word")).select(
        col("word"), lit(1).count
    ).execute_insert("sink").wait()


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", dest="input", required=False, help="Input file to process.")
    parser.add_argument(
        "--output", dest="output", required=False, help="Output file to write results to."
    )

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    word_count(known_args.input, known_args.output)
