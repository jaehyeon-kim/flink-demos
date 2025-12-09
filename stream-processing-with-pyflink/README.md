# Stream Processing with Flink in Python

This project contains Python implementations of examples for learning Apache Flink, inspired by the [`streaming-with-flink` Scala](https://github.com/streaming-with-flink/examples-scala) project.

## Getting Started

First, clone the repository to your local machine:

```bash
git clone https://github.com/jaehyeon-kim/flink-demos.git
cd flink-demos/stream-processing-with-pyflink
```

It is recommended to create a virtual environment to manage the project's dependencies.

```bash
python -m venv venv
source venv/bin/activate
```

Next, install the required Python packages, which are listed in the `requirements.txt` file.

```bash
pip install -r requirements-dev.txt
```

## Running the Examples

You can run the Flink jobs locally from your command line. Use the `python` command to execute the individual example scripts.

Here are a few examples from different chapters. Please check the `src` directory for all available applications.

```bash
# Run an example from Chapter 5
python src/chapter5/basic_transformations.py

# Run an example from Chapter 6
python src/chapter6/process_function_timers.py

# Run an example from Chapter 7
python src/chapter7/keyed_state_function.py
```
