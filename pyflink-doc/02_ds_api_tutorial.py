import argparse
import logging
import sys

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, DataStream
from pyflink.datastream.connectors.file_system import (
    FileSource,
    StreamFormat,
    FileSink,
    OutputFileConfig,
    RollingPolicy,
)

from data import word_count_data


def create_input_stream(env: StreamExecutionEnvironment, input_path):
    if input_path is not None:
        return env.from_source(
            source=FileSource.for_record_stream_format(StreamFormat.text_line_format(), input_path)
            .process_static_file_set()
            .build(),
            watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
            source_name="file_source",
        )
    else:
        print("Executing word_count example with default input data set.")
        print("Use --input to specify file input.")
        return env.from_collection(word_count_data)


def process_ds_sink(ds: DataStream, output_path):
    if output_path is not None:
        ds.sink_to(
            sink=FileSink.for_row_format(
                base_path=output_path, encoder=Encoder.simple_string_encoder()
            )
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("prefix")
                .with_part_suffix(".ext")
                .build()
            )
            .with_rolling_policy(RollingPolicy.default_rolling_policy())
            .build()
        )
    else:
        print("Printing result to stdout. Use --output to specify output path.")
        ds.print()


def word_count(input_path, output_path):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    env.set_parallelism(1)

    ds = create_input_stream(env, input_path)

    def split(line: str):
        yield from line.split()

    ds = (
        ds.flat_map(split)
        .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
        .key_by(lambda i: i[0])
        .reduce(lambda i, j: (i[0], i[1] + j[1]))
    )

    process_ds_sink(ds, output_path)

    env.execute()


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
