package me.jaehyeon.chapter6

import me.jaehyeon.sensor.SensorReading
import me.jaehyeon.sensor.SensorSource
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.time.Duration
import kotlin.system.exitProcess

/**
 * This Flink job demonstrates various windowed computations on a keyed DataStream.
 *
 * It showcases a pipeline that:
 * 1. **Source**: Ingests `SensorReading` events, assigning timestamps and watermarks for event-time processing.
 * 2. **Keying**: Keys the stream by sensor ID.
 * 3. **Windowing**: Applies 5-second tumbling event-time windows.
 * 4. **Applying Window Functions**: Demonstrates five different ways to process data within the windows:
 *    - `reduce` with a lambda for simple, incremental aggregation.
 *    - `reduce` with a `ReduceFunction` class implementation.
 *    - `aggregate` with an `AggregateFunction` for more complex incremental aggregation (like averaging).
 *    - `process` with a `ProcessWindowFunction` to get full access to window contents and metadata (less efficient as it buffers).
 *    - A combination of `reduce` and `process` for efficient, incremental pre-aggregation followed by enrichment with window metadata.
 * 5. **Sink**: Prints the results of the final, most efficient approach to the console.
 */
object WindowFunctions {
    @JvmStatic
    fun main(args: Array<String>) {
        // Check for a command-line argument to determine which mode to run.
        if (args.isEmpty()) {
            println("Please provide a mode: 'min1', 'min2', 'avg', 'minmax1', or 'minmax2'")
        }
        val mode = args[0]

        // Set up the streaming execution environment
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        // Configure checkpointing and watermark interval
        env.checkpointConfig.checkpointInterval = 10 * 1000
        env.config.autoWatermarkInterval = 1000L

        // Ingest sensor stream and assign timestamps and watermarks
        val readings =
            env.fromSource(
                SensorSource(),
                WatermarkStrategy
                    .forBoundedOutOfOrderness<SensorReading>(Duration.ofSeconds(5))
                    .withTimestampAssigner { reading, _ ->
                        reading.timestamp
                    },
                "Sensor Source",
            )

        // --- Prepare several common windowed streams for different function examples ---

        // A windowed stream of (id, temperature) tuples
        val windowedTuple2Stream =
            readings
                .map { Tuple2(it.id, it.temperature) }
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE))
                .keyBy { it.f0 }
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))

        // A windowed stream of (id, temp, temp) for min/max reduce calculation
        val windowedTuple3Stream =
            readings
                // Map to (id, temp, temp) because ReduceFunction requires input and output types to be the same
                .map { Tuple3(it.id, it.temperature, it.temperature) }
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE, Types.DOUBLE))
                .keyBy { it.f0 }
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))

        // A windowed stream of the original SensorReading objects
        val windowedSensorStream =
            readings
                .keyBy { it.id }
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))

        // Call the appropriate function based on the command-line argument.
        when (mode) {
            "min1" -> {
                // --- 1. ReduceFunction with a lambda ---
                // Incrementally computes the minimum temperature per window.
                val minTempPerWindow1 = windowedTuple2Stream.reduce { r1, r2 -> Tuple2(r1.f0, minOf(r1.f1, r2.f1)) }
                minTempPerWindow1.print()
            }

            "min2" -> {
                // --- 2. ReduceFunction with a class implementation ---
                // Same as above, but logic is encapsulated in a dedicated class.
                val minTempPerWindow2 = windowedTuple2Stream.reduce(MinTempFunction())
                minTempPerWindow2.print()
            }

            "avg" -> {
                // --- 3. AggregateFunction ---
                // Incrementally computes the average temperature per window. More flexible than reduce.
                val avgTempPerWindow = windowedTuple2Stream.aggregate(AvgTempFunction())
                avgTempPerWindow.print()
            }

            "minmax1" -> {
                // --- 4. ProcessWindowFunction ---
                // Finds the min and max temperature by iterating over all elements buffered for a window.
                // Powerful but less efficient for simple aggregations.
                val minMaxTempPerWindow = windowedSensorStream.process(HighAndLowTempProcessFunction())
                minMaxTempPerWindow.print()
            }

            "minmax2" -> {
                // --- 5. Incremental Aggregation with ReduceFunction and ProcessWindowFunction ---
                // The most efficient approach: pre-aggregate with ReduceFunction, then add window info with ProcessWindowFunction.
                val minMaxTempPerWindow2 =
                    windowedTuple3Stream
                        .reduce(MinMaxReduceFunction(), AssignWindowEndProcessFunction())
                minMaxTempPerWindow2.print()
            }

            else -> {
                println("Unknown mode: $mode. Please use 'filter', 'sideoutput', or 'update'")
                exitProcess(1)
            }
        }

        env.execute("Execute window functions")
    }
}

/** A ReduceFunction that computes the minimum temperature from a stream of (id, temperature) tuples. */
class MinTempFunction : ReduceFunction<Tuple2<String, Double>> {
    override fun reduce(
        value1: Tuple2<String, Double>,
        value2: Tuple2<String, Double>,
    ): Tuple2<String, Double> = Tuple2(value1.f0, minOf(value1.f1, value2.f1))
}

/** A ReduceFunction that incrementally finds the minimum and maximum temperature. */
class MinMaxReduceFunction : ReduceFunction<Tuple3<String, Double, Double>> {
    override fun reduce(
        value1: Tuple3<String, Double, Double>,
        value2: Tuple3<String, Double, Double>,
    ): Tuple3<String, Double, Double> = Tuple3(value1.f0, minOf(value1.f1, value2.f1), maxOf(value1.f2, value2.f2))
}

/**
 * An AggregateFunction to compute the average temperature.
 * IN: Tuple2<String, Double> - (id, temperature)
 * ACC: Tuple3<String, Double, Int> - (id, sum of temperatures, count of readings)
 * OUT: Tuple2<String, Double> - (id, average temperature)
 */
class AvgTempFunction : AggregateFunction<Tuple2<String, Double>, Tuple3<String, Double, Int>, Tuple2<String, Double>> {
    override fun createAccumulator(): Tuple3<String, Double, Int> = Tuple3("", 0.0, 0)

    override fun add(
        value: Tuple2<String, Double>,
        accumulator: Tuple3<String, Double, Int>,
    ): Tuple3<String, Double, Int> = Tuple3(value.f0, value.f1 + accumulator.f1, 1 + accumulator.f2)

    override fun merge(
        a: Tuple3<String, Double, Int>,
        b: Tuple3<String, Double, Int>,
    ): Tuple3<String, Double, Int> = Tuple3(a.f0, a.f1 + b.f1, a.f2 + b.f2)

    override fun getResult(accumulator: Tuple3<String, Double, Int>): Tuple2<String, Double> =
        Tuple2(accumulator.f0, accumulator.f1 / accumulator.f2)
}

/** Data class to hold the result of the min/max temperature calculation for a window. */
data class MinMaxTemp(
    val id: String,
    val min: Double,
    val max: Double,
    val endTs: Long,
)

/**
 * A ProcessWindowFunction that computes the lowest and highest temperature by iterating
 * over all elements buffered for a window. It also adds the window's end timestamp.
 */
class HighAndLowTempProcessFunction : ProcessWindowFunction<SensorReading, MinMaxTemp, String, TimeWindow>() {
    override fun process(
        key: String,
        context: Context,
        elements: Iterable<SensorReading>,
        out: Collector<MinMaxTemp>,
    ) {
        val temps = elements.map { it.temperature }
        val windowEnd = context.window().end
        out.collect(MinMaxTemp(key, temps.minOrNull() ?: 0.0, temps.maxOrNull() ?: 0.0, windowEnd))
    }
}

/**
 * A ProcessWindowFunction that receives a single, pre-aggregated result from a ReduceFunction
 * and enriches it with the window's end timestamp.
 */
class AssignWindowEndProcessFunction : ProcessWindowFunction<Tuple3<String, Double, Double>, MinMaxTemp, String, TimeWindow>() {
    override fun process(
        key: String,
        context: Context,
        elements: Iterable<Tuple3<String, Double, Double>>,
        out: Collector<MinMaxTemp>,
    ) {
        // The iterable will contain exactly one element: the result of the ReduceFunction.
        val minMax = elements.first()
        val windowEnd = context.window().end
        out.collect(MinMaxTemp(key, minMax.f1, minMax.f2, windowEnd))
    }
}
