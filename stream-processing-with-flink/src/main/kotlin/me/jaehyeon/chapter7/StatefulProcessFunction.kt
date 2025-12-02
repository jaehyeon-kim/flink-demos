package me.jaehyeon.chapter7

import me.jaehyeon.sensor.SensorReading
import me.jaehyeon.sensor.SensorSource
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import java.time.Duration
import kotlin.math.abs

/**
 * This Flink job demonstrates a stateful ProcessFunction with timer-based state cleaning.
 *
 * It monitors a stream of sensor readings and identifies significant temperature jumps for each sensor.
 * To prevent state from growing indefinitely for sensors that are no longer active, this job
 * implements a self-cleaning mechanism. An event-time timer is registered for each sensor, and if
 * no new data arrives within a specified period (e.g., one hour), the timer triggers and clears
 * the state associated with that sensor.
 *
 * This example pipeline:
 * 1. Ingests a stream of `SensorReading` events with watermarks.
 * 2. Keys the stream by sensor ID.
 * 3. Applies a `KeyedProcessFunction` (`SelfCleaningTemperatureAlertFunction`) to:
 *    a. Store the last-seen temperature for each sensor.
 *    b. Emit an alert if the temperature change between consecutive readings exceeds a threshold.
 *    c. Register a one-hour timer to clear the sensor's state if it becomes inactive.
 * 4. Prints the resulting alert stream to the console.
 */
object StatefulProcessFunction {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.checkpointConfig.checkpointInterval = 10 * 1000
        env.config.autoWatermarkInterval = 1000L

        // Ingest the stream of sensor readings.
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

        // Apply the stateful process function to generate alerts.
        val alerts =
            readings
                .keyBy { it.id }
                .process(SelfCleaningTemperatureAlertFunction(1.5))

        // Print the alerts to the console.
        alerts.print()

        env.execute("Generate Temperature Alerts")
    }
}

/**
 * A KeyedProcessFunction that issues an alert if the temperature difference between
 * consecutive readings for the same sensor exceeds a given threshold. This function also
 * cleans up its state if a sensor remains inactive for one hour.
 *
 * @param threshold The temperature difference that triggers an alert.
 */
class SelfCleaningTemperatureAlertFunction(
    private val threshold: Double,
) : KeyedProcessFunction<String, SensorReading, Tuple3<String, Double, Double>>() {
    // State to store the last recorded temperature for the current key.
    private lateinit var lastTempState: ValueState<Double>

    // State to store the timestamp of the last registered cleanup timer.
    private lateinit var lastTimerState: ValueState<Long>

    override fun open(parameters: Configuration) {
        // Initialize state descriptors. These are used to retrieve the state handles.
        val lastTempDescriptor = ValueStateDescriptor("lastTemp", Types.DOUBLE)
        lastTempState = runtimeContext.getState(lastTempDescriptor)

        val timestampDescriptor = ValueStateDescriptor("timestampState", Types.LONG)
        lastTimerState = runtimeContext.getState(timestampDescriptor)
    }

    override fun processElement(
        value: SensorReading,
        ctx: Context,
        out: Collector<Tuple3<String, Double, Double>>,
    ) {
        // Set a new timer for one hour from the current event's timestamp.
        // This timer will trigger the onTimer() method if no new element arrives for this key
        // within the next hour, allowing us to clean up the state.
        val newTimer = ctx.timestamp() + (3600 * 1000) // 1 hour in milliseconds
        // Retrieve the timestamp of the previously registered timer.
        val curTimer = lastTimerState.value() ?: 0L

        // Delete the previous timer and register the new one. This effectively resets the
        // inactivity countdown for this sensor.
        ctx.timerService().deleteEventTimeTimer(curTimer)
        ctx.timerService().registerEventTimeTimer(newTimer)
        lastTimerState.update(newTimer)

        // Retrieve the last known temperature for this sensor.
        val lastTemp = lastTempState.value() ?: 0.0
        val tempDiff = abs(value.temperature - lastTemp)

        // If the temperature jump is significant, emit an alert.
        if (tempDiff > threshold) {
            out.collect(Tuple3(value.id, value.temperature, tempDiff))
        }

        // Update the state with the current temperature for the next comparison.
        lastTempState.update(value.temperature)
    }

    /**
     * This method is invoked when an event-time timer, registered in `processElement`, fires.
     * It indicates that the sensor has been inactive for the specified duration (one hour).
     */
    override fun onTimer(
        timestamp: Long,
        ctx: OnTimerContext,
        out: Collector<Tuple3<String, Double, Double>>,
    ) {
        // Clear all state associated with this key (sensor) to free up resources.
        lastTempState.clear()
        lastTimerState.clear()
    }
}
