package me.jaehyeon.chapter6

import me.jaehyeon.sensor.SensorReading
import me.jaehyeon.sensor.SensorSource
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.api.java.tuple.Tuple4
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag
import java.time.Duration
import java.util.Random
import kotlin.system.exitProcess

/**
 * This Flink job demonstrates three different strategies for handling late data.
 *
 * It intentionally creates out-of-order data and then applies one of the following
 * techniques based on a command-line argument:
 *
 * 1.  **filter**: A `ProcessFunction` manually filters late events into a side output before any windowing.
 * 2.  **sideoutput**: Uses the built-in `.sideOutputLateData()` operator on a window to divert late events.
 * 3.  **update**: Uses `.allowedLateness()` to keep a window alive and process late events as updates to the window's result.
 */
object LateDataHandling {
    // A shared OutputTag for diverting late SensorReadings.
    // The anonymous inner class `object : ... {}` is required to preserve the generic type.
    val lateReadingsOutput: OutputTag<SensorReading> = object : OutputTag<SensorReading>("late-readings") {}

    @JvmStatic
    fun main(args: Array<String>) {
        // Check for a command-line argument to determine which mode to run.
        if (args.isEmpty()) {
            println("Please provide a mode: 'filter', 'sideoutput', or 'update'")
            exitProcess(1)
        }
        val mode = args[0]

        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.checkpointConfig.checkpointInterval = 10 * 1000
        env.config.autoWatermarkInterval = 500L

        // Ingest a stream of sensor readings.
        val readings =
            env.fromSource(
                SensorSource(),
                WatermarkStrategy.noWatermarks(), // Watermarks will be assigned after shuffling.
                "Sensor Source",
            )

        // Intentionally shuffle timestamps to create out-of-order data for the demonstration.
        val outOfOrderReadings =
            readings
                .map(TimestampShuffler(7 * 1000))
                .assignTimestampsAndWatermarks(
                    // Define a watermark strategy that allows for 5 seconds of lateness.
                    WatermarkStrategy
                        .forBoundedOutOfOrderness<SensorReading>(Duration.ofSeconds(5))
                        .withTimestampAssigner { reading, _ ->
                            reading.timestamp
                        },
                )

        // Call the appropriate function based on the command-line argument.
        when (mode) {
            "filter" -> {
                filterLateReadings(outOfOrderReadings)
            }

            "sideoutput" -> {
                sideOutputLateEventsWindow(outOfOrderReadings, 10L)
            }

            "update" -> {
                updateForLateEventsWindow(outOfOrderReadings, 10L)
            }

            else -> {
            }
        }

        env.execute("Late Data Handling Example: $mode")
    }

    /**
     * Strategy 1: Manually filter late readings using a ProcessFunction before windowing.
     */
    fun filterLateReadings(readings: DataStream<SensorReading>) {
        val filteredReadings = readings.process(LateReadingsFilter())
        val lateReadings = filteredReadings.getSideOutput(lateReadingsOutput)

        filteredReadings.print().name("On-Time Readings")
        lateReadings
            .map { r -> "*** LATE READING (FILTER) *** ${r.id}" }
            .returns(Types.STRING)
            .print()
            .name("Late Readings (Filter)")
    }

    /**
     * Strategy 2: Use the built-in .sideOutputLateData() operator on a window.
     */
    fun sideOutputLateEventsWindow(
        readings: DataStream<SensorReading>,
        windowSec: Long,
    ) {
        val countPerWindowSec =
            readings
                .keyBy { it.id }
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(windowSec)))
                .sideOutputLateData(lateReadingsOutput) // Divert late data here.
                .process(CountLateReadings())

        countPerWindowSec
            .getSideOutput(lateReadingsOutput)
            .map { r -> "*** LATE READING (SIDE OUTPUT) *** ${r.id}" }
            .returns(Types.STRING)
            .print()
            .name("Late Readings (Side Output)")

        countPerWindowSec.print().name("On-Time Window Counts")
    }

    /**
     * Strategy 3: Use .allowedLateness() to update window results with late data.
     */
    fun updateForLateEventsWindow(
        readings: DataStream<SensorReading>,
        windowSec: Long,
    ) {
        readings
            .keyBy { it.id }
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(windowSec)))
            .allowedLateness(Duration.ofSeconds(5)) // Keep window state for 5 extra seconds.
            .process(UpdatingWindowCountFunction())
            .print()
            .name("Updated Window Counts")
    }
}

/**
 * A ProcessFunction that checks if a reading's timestamp is before the current watermark.
 * If so, it's considered late and sent to a side output. Otherwise, it's sent to the main output.
 */
class LateReadingsFilter : ProcessFunction<SensorReading, SensorReading>() {
    override fun processElement(
        value: SensorReading,
        ctx: Context,
        out: Collector<SensorReading>,
    ) {
        if (value.timestamp < ctx.timerService().currentWatermark()) {
            // This event is late; send it to the side output.
            ctx.output(LateDataHandling.lateReadingsOutput, value)
        } else {
            // This event is on-time; send it to the main output.
            out.collect(value)
        }
    }
}

/**
 * A simple ProcessWindowFunction that counts on-time elements in a window.
 */
class CountLateReadings : ProcessWindowFunction<SensorReading, Tuple3<String, Long, Int>, String, TimeWindow>() {
    override fun process(
        key: String,
        context: Context,
        elements: Iterable<SensorReading>,
        out: Collector<Tuple3<String, Long, Int>>,
    ) {
        // Emits (key, windowEnd, count)
        out.collect(Tuple3(key, context.window().end, elements.count()))
    }
}

/**
 * A ProcessWindowFunction that uses managed state to flag if a window result is an update.
 * This is used with .allowedLateness() to show how late events can trigger new results for the same window.
 */
class UpdatingWindowCountFunction : ProcessWindowFunction<SensorReading, Tuple4<String, Long, Int, String>, String, TimeWindow>() {
    override fun process(
        key: String,
        context: Context,
        elements: Iterable<SensorReading>,
        out: Collector<Tuple4<String, Long, Int, String>>,
    ) {
        val count = elements.count()

        // Use window state to check if this is the first time the window is being processed.
        val isUpdateState =
            context.windowState().getState(
                ValueStateDescriptor("isUpdate", Types.BOOLEAN),
            )

        val isUpdate = isUpdateState.value() ?: false

        if (!isUpdate) {
            // First time processing this window.
            out.collect(Tuple4(key, context.window().end, count, "first"))
            isUpdateState.update(true) // Mark that this window has been processed.
        } else {
            // A late event arrived, triggering a subsequent processing.
            out.collect(Tuple4(key, context.window().end, count, "update"))
        }
    }
}

/**
 * A MapFunction that randomly shuffles the timestamp of a SensorReading
 * to simulate out-of-order data.
 */
class TimestampShuffler(
    private val maxRandomOffset: Int,
) : MapFunction<SensorReading, SensorReading> {
    @Transient // Ensure the Random object is not serialized during checkpointing.
    private lateinit var rand: Random

    override fun map(value: SensorReading): SensorReading {
        // Lazily initialize Random to avoid serialization issues.
        if (!this::rand.isInitialized) {
            rand = Random()
        }

        // Add a random positive offset to the original timestamp.
        val shuffleTs = value.timestamp + rand.nextInt(maxRandomOffset)
        return SensorReading(value.id, shuffleTs, value.temperature)
    }
}
