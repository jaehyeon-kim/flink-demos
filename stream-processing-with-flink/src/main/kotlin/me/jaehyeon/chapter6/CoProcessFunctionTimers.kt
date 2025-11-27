package me.jaehyeon.chapter6

import me.jaehyeon.sensor.SensorReading
import me.jaehyeon.sensor.SensorSource
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.datagen.source.DataGeneratorSource
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector

/**
 * This Flink job demonstrates a `KeyedCoProcessFunction` to implement a dynamically
 * configurable filter.
 *
 * The goal is to forward sensor readings for a specific sensor only for a limited
 * period of time, which is controlled by a second "control" stream.
 *
 * The pipeline works as follows:
 * 1. **Sensor Stream**: A high-volume stream of `SensorReading` events.
 * 2. **Control Stream**: A low-volume stream of `Tuple2<String, Long>` which represents
 *    filter commands: `(sensorId, durationInMillis)`.
 * 3. **KeyBy & Connect**: BOTH streams are keyed by the sensor ID. This is crucial as it
 *    ensures that a filter command for "sensor_2" is processed on the same physical
 *    task that handles readings for "sensor_2".
 * 4. **KeyedCoProcessFunction (`ReadingFilter`)**:
 *    - When a filter command arrives, it enables forwarding for that key and sets a
 *      processing time timer to disable it later.
 *    - When a sensor reading arrives, it checks the state for that key. If forwarding is
 *      enabled, the reading is passed through; otherwise, it is dropped.
 *    - When a timer fires, it disables forwarding for that key.
 */
object CoProcessFunctionTimers {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        // Source for sensor readings (using processing time)
        val readings =
            env.fromSource(
                SensorSource(),
                WatermarkStrategy.noWatermarks(),
                "Sensor Source",
            )

        // Source for filter switch commands: (sensorId, duration)
        val filterSwitches = FilterSwitch.createFilterSwitchSource(env)

        // Key BOTH streams by sensor ID and then connect them
        val forwardedReadings =
            readings
                .keyBy { it.id }
                .connect(filterSwitches.keyBy { it.f0 })
                .process(ReadingFilter())

        forwardedReadings.print()

        env.execute("CoProcessFunction Timers Example")
    }
}

/**
 * A KeyedCoProcessFunction that forwards readings for a given key for a limited
 * time based on commands from a second stream.
 */
class ReadingFilter : KeyedCoProcessFunction<String, SensorReading, Tuple2<String, Long>, SensorReading>() {
    // State to track whether forwarding is currently enabled for this key
    private lateinit var forwardingEnabled: ValueState<Boolean>

    // State to track the timestamp of the disable timer for this key
    private lateinit var disableTimer: ValueState<Long>

    override fun open(parameters: Configuration) {
        // Initialize state descriptors
        val forwardingEnabledDescriptor = ValueStateDescriptor("forwardingEnabled", Types.BOOLEAN)
        forwardingEnabled = runtimeContext.getState(forwardingEnabledDescriptor)

        val disableTimerDescriptor = ValueStateDescriptor("disableTimer", Types.LONG)
        disableTimer = runtimeContext.getState(disableTimerDescriptor)
    }

    /** This method is called for each element from the FIRST stream (SensorReadings). */
    override fun processElement1(
        value: SensorReading,
        ctx: Context,
        out: Collector<SensorReading>,
    ) {
        // Check if forwarding is enabled for the current key (defaults to false if null)
        val isForwarding = forwardingEnabled.value() ?: false
        if (isForwarding) {
            out.collect(value)
        }
    }

    /** This method is called for each element from the SECOND stream (filter commands). */
    override fun processElement2(
        value: Tuple2<String, Long>,
        ctx: Context,
        out: Collector<SensorReading>,
    ) {
        // A filter command arrives, so enable forwarding for this key
        forwardingEnabled.update(true)

        // Calculate the timestamp when forwarding should be disabled
        val timerTimestamp = ctx.timerService().currentProcessingTime() + value.f1
        val curTimerTimestamp = disableTimer.value() ?: 0L

        // Check if the new timer should be later than any existing timer
        if (timerTimestamp > curTimerTimestamp) {
            // If there's an old timer, delete it
            if (curTimerTimestamp != 0L) {
                ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
            }
            // Register the new timer and store its timestamp in state
            ctx.timerService().registerProcessingTimeTimer(timerTimestamp)
            disableTimer.update(timerTimestamp)
        }
    }

    /** This method is called when a processing time timer fires for a key. */
    override fun onTimer(
        timestamp: Long,
        ctx: OnTimerContext,
        out: Collector<SensorReading>,
    ) {
        // The timer has fired, so we disable forwarding and clear the state for this key
        forwardingEnabled.clear()
        disableTimer.clear()
    }
}

/**
 * A factory object for creating filter switch sources.
 * Encapsulating this logic keeps the main job definition clean and focused.
 */
object FilterSwitch {
    /**
     * Creates a stream of Tuple2<String, Long> filter commands.
     * Elements are emitted with a 1-second delay between them to simulate a real-world scenario.
     */
    fun createFilterSwitchSource(env: StreamExecutionEnvironment): DataStreamSource<Tuple2<String, Long>> {
        val filterCommands =
            listOf(
                Tuple2("sensor_2", 5 * 1000L),
                Tuple2("sensor_7", 6 * 1000L),
                Tuple2("sensor_2", 10 * 1000L),
            )

        val generatorSource =
            DataGeneratorSource(
                { index -> filterCommands[index.toInt()] },
                filterCommands.size.toLong(),
                RateLimiterStrategy.perSecond(1.0),
                Types.TUPLE(Types.STRING, Types.LONG),
            )

        return env.fromSource(
            generatorSource,
            WatermarkStrategy.noWatermarks(),
            "Filter Switch Generator",
        )
    }
}
