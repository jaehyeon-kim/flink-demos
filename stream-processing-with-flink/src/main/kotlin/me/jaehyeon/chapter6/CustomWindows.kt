package me.jaehyeon.chapter6

import me.jaehyeon.sensor.SensorReading
import me.jaehyeon.sensor.SensorSource
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.tuple.Tuple4
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.time.Duration

/**
 * This Flink job demonstrates how to implement and use custom windowing logic.
 *
 * The pipeline consists of:
 * 1. A `CustomTumblingWindows` assigner that groups events into windows of a configurable size.
 * 2. A `CustomIntervalTrigger` that fires periodically before the window closes, providing early,
 *    intermediate results.
 * 3. A `ProcessWindowFunction` that counts the events for each triggered evaluation.
 */
object CustomWindows {
    @JvmStatic
    fun main(args: Array<String>) {
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

        val countsWithCustomInterval =
            readings
                .keyBy { it.id }
                // Assign elements to 5-second tumbling windows
                .window(CustomTumblingWindows(5000L))
                // Use a trigger that fires every 1 second for early results
                .trigger(CustomIntervalTrigger(1000L))
                // Count elements in each triggered pane
                .process(CountFunction())

        countsWithCustomInterval.print()

        env.execute("Custom Windows Job")
    }
}

/**
 * A custom Flink WindowAssigner that groups elements into non-overlapping (tumbling)
 * event-time windows of a configurable size.
 */
class CustomTumblingWindows(
    private val windowSize: Long = 30 * 1000L,
) : WindowAssigner<Any, TimeWindow>() {
    /**
     * Assigns an element to a single time window based on its timestamp by rounding
     * the timestamp down to the nearest multiple of the window size.
     * e.g. windowSize = 30000, timestamp = 47215
     *      startTime = 47215 - (47215 % 30000) = 30000
     *      endTime = 30000 + 30000 = 60000
     */
    override fun assignWindows(
        element: Any,
        timestamp: Long,
        context: WindowAssignerContext,
    ): List<TimeWindow> {
        val startTime = timestamp - (timestamp % windowSize)
        val endTime = startTime + windowSize
        return listOf(TimeWindow(startTime, endTime))
    }

    /**
     * Returns the default trigger. This method overrides a deprecated Flink API,
     * so it must also be marked as @Deprecated.
     */
    @Deprecated("Overrides deprecated member in superclass.")
    override fun getDefaultTrigger(env: StreamExecutionEnvironment): Trigger<Any, TimeWindow> = EventTimeTrigger.create()

    /**
     * Returns the serializer for the TimeWindow type.
     */
    override fun getWindowSerializer(executionConfig: ExecutionConfig): TypeSerializer<TimeWindow> = TimeWindow.Serializer()

    /**
     * Returns true, indicating this assigner uses event time. This abstract method
     * must be implemented even though it is deprecated.
     */
    override fun isEventTime(): Boolean = true
}

/**
 * An event-time trigger that provides early, periodic results. It fires at a defined interval
 * as the watermark advances, and also fires a final time when the window ends.
 */
class CustomIntervalTrigger(
    private val triggerInterval: Long = 1000L,
) : Trigger<SensorReading, TimeWindow>() {
    /**
     * State descriptor for a flag that tracks if we have seen the first element for a window.
     * This is initialized as a member variable to be robust against job restarts from checkpoints.
     */
    private val firstSeenDescriptor = ValueStateDescriptor("firstSeen", Types.BOOLEAN)

    /**
     * Called for every element that is assigned to a window.
     * It sets timers only for the very first element to avoid redundant timer registration.
     */
    override fun onElement(
        element: SensorReading,
        timestamp: Long,
        window: TimeWindow,
        ctx: TriggerContext,
    ): TriggerResult {
        // Get the partitioned state for the current key and window
        val firstSeen: ValueState<Boolean> = ctx.getPartitionedState(firstSeenDescriptor)

        // If this is the first element, register timers.
        if (firstSeen.value() != true) {
            // Calculate the time for the next periodic firing.
            // This rounds the current watermark up to the next interval boundary.
            val t = ctx.currentWatermark + (triggerInterval - (ctx.currentWatermark % triggerInterval))
            ctx.registerEventTimeTimer(t)
            // Register a timer for the end of the window to ensure a final firing.
            ctx.registerEventTimeTimer(window.end)
            // Set the flag to true so other elements won't re-register timers.
            firstSeen.update(true)
        }
        // We never fire per-element, so we always continue.
        return TriggerResult.CONTINUE
    }

    /**
     * Called when a previously registered event-time timer fires.
     */
    override fun onEventTime(
        time: Long,
        window: TimeWindow,
        ctx: TriggerContext,
    ): TriggerResult =
        if (time == window.end) {
            // If the timer is for the end of the window, fire and purge all state.
            TriggerResult.FIRE_AND_PURGE
        } else {
            // It's an early, periodic firing.
            // Schedule the next periodic timer.
            val t = ctx.currentWatermark + (triggerInterval - (ctx.currentWatermark % triggerInterval))
            if (t < window.end) {
                ctx.registerEventTimeTimer(t)
            }
            // Fire to produce an intermediate result. The window state is preserved.
            TriggerResult.FIRE
        }

    /** This trigger doesn't use processing time, so it does nothing. */
    override fun onProcessingTime(
        time: Long,
        window: TimeWindow,
        ctx: TriggerContext,
    ): TriggerResult = TriggerResult.CONTINUE

    /** Called when the window is purged. We must clear our managed state. */
    override fun clear(
        window: TimeWindow,
        ctx: TriggerContext,
    ) {
        val firstSeen: ValueState<Boolean> = ctx.getPartitionedState(firstSeenDescriptor)
        firstSeen.clear()
    }
}

/**
 * A simple ProcessWindowFunction that counts the number of elements in the window for a given
 * firing and emits the result along with window metadata.
 */
class CountFunction : ProcessWindowFunction<SensorReading, Tuple4<String, Long, Long, Int>, String, TimeWindow>() {
    override fun process(
        key: String,
        context: Context,
        elements: Iterable<SensorReading>,
        out: Collector<Tuple4<String, Long, Long, Int>>,
    ) {
        // Count the elements in the current window pane.
        val count = elements.count()
        // Emit a tuple of (key, windowEnd, currentWatermark, elementCount).
        out.collect(Tuple4(key, context.window().end, context.currentWatermark(), count))
    }
}
