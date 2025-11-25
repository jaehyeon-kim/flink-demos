package me.jaehyeon.chapter1

import me.jaehyeon.sensor.SensorReading
import me.jaehyeon.sensor.SensorSource
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.time.Duration

/**
 * Defines and executes the Flink DataStream program to compute average sensor temperatures.
 *
 * The main entry point is within the `main` function, which is annotated with @JvmStatic
 * to allow it to be called as a standard Java main method by Gradle.
 */
object AverageSensorReadings {
    @JvmStatic
    fun main(args: Array<String>) {
        // 1. Set up the streaming execution environment.
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        // 2. Create the data source.
        // We use the modern `fromSource` API with our custom `SensorSource`.
        val sensorData =
            env.fromSource(
                SensorSource(),
                // Define the watermark strategy. Event time is now the default and is configured here.
                // We specify a 5-second bounded out-of-orderness tolerance.
                WatermarkStrategy
                    .forBoundedOutOfOrderness<SensorReading>(Duration.ofSeconds(5))
                    // Tell Flink how to extract the timestamp from each SensorReading event.
                    .withTimestampAssigner { reading, _ -> reading.timestamp },
                "Sensor Source",
            )

        // 3. Define the data processing pipeline.
        val avgTemp =
            sensorData
                // Apply a simple map transformation to convert temperatures from Fahrenheit to Celsius.
                .map { r ->
                    SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0))
                }
                // Partition the stream by the sensor ID. All subsequent operations will be
                // performed independently for each sensor.
                .keyBy { it.id }
                // Define a 1-second tumbling window based on event time.
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(1)))
                // Apply a window computation. Using an AggregateFunction together with a
                // ProcessWindowFunction is the most efficient approach. The AggregateFunction
                // pre-computes the average, and the ProcessWindowFunction formats the final output.
                .aggregate(TemperatureAggregator(), TemperatureAverager())

        // 4. Sink the results to the console.
        avgTemp.print()

        // 5. Execute the Flink job.
        env.execute("Compute average sensor temperature")
    }
}

/**
 * An `AggregateFunction` for efficiently computing the average temperature within a window.
 *
 * It incrementally calculates the count and sum of temperatures as events arrive, storing
 * only a single accumulator value per window. This is far more memory-efficient than
 * collecting all events in the window.
 *
 * - IN: The input type (`SensorReading`).
 * - ACC: The accumulator type (`Pair<Int, Double>` for count and sum).
 * - OUT: The output type (`Double` for the final average).
 */
class TemperatureAggregator : AggregateFunction<SensorReading, Pair<Int, Double>, Double> {
    // Initialize the accumulator: (count=0, sum=0.0).
    override fun createAccumulator(): Pair<Int, Double> = 0 to 0.0

    // Add a new reading to the accumulator.
    override fun add(
        value: SensorReading,
        accumulator: Pair<Int, Double>,
    ): Pair<Int, Double> = (accumulator.first + 1) to (accumulator.second + value.temperature)

    // Calculate the final result from the accumulator.
    override fun getResult(accumulator: Pair<Int, Double>): Double {
        // Avoid division by zero for empty windows.
        return if (accumulator.first == 0) 0.0 else accumulator.second / accumulator.first
    }

    // Merge two accumulators (used in session windows, but must be implemented).
    override fun merge(
        a: Pair<Int, Double>,
        b: Pair<Int, Double>,
    ): Pair<Int, Double> = (a.first + b.first) to (a.second + b.second)
}

/**
 * A `ProcessWindowFunction` that formats the output of the `TemperatureAggregator`.
 *
 * This function receives the final average temperature computed by the aggregator and
 * enriches it with metadata from the window, such as the sensor ID (the key) and
 * the window's end time.
 *
 * - IN: The input from the aggregator (`Double`).
 * - OUT: The final output type (`SensorReading`).
 * - KEY: The key of the stream (`String`).
 * - W: The type of the window (`TimeWindow`).
 */
class TemperatureAverager : ProcessWindowFunction<Double, SensorReading, String, TimeWindow>() {
    override fun process(
        key: String,
        context: Context,
        elements: Iterable<Double>,
        out: Collector<SensorReading>,
    ) {
        // The 'elements' iterable will contain exactly one element: the result from the aggregator.
        val avgTemp = elements.first()
        // Create a new SensorReading with the key, window end time, and the computed average.
        out.collect(SensorReading(key, context.window().end, avgTemp))
    }
}
