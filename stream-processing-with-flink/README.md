# Stream Processing with Flink (Kotlin Examples)

This project contains Kotlin implementations of examples for learning Apache Flink, inspired by the [`streaming-with-flink` Scala](<(https://github.com/streaming-with-flink/examples-scala)>) project.

## Building the Project

To compile the code, run all tests, and create a deployable "fat" JAR, use the `build` task.

```bash
./gradlew build
```

The final JAR file will be located at `build/libs/stream-processing-with-flink-1.0.jar`. This is the file you would submit to a Flink cluster for execution. It contains your application code and its required dependencies, but excludes the Flink runtime libraries themselves to avoid classpath conflicts.

## Running Locally for Development

You can run your Flink jobs directly from the command line for local testing and development. This is useful for quick debugging without needing a full Flink cluster.

Use the `run` task and specify which main class you want to execute using the `-PmainClass` project property.

```bash
# Run the example from Chapter 1
./gradlew run -PmainClass=me.jaehyeon.chapter1.AverageSensorReadings
```

## Running Tests

To execute all unit tests in the project, use the `test` task.

```bash
./gradlew test
```

A test report will be generated at `build/reports/tests/test/index.html`.
