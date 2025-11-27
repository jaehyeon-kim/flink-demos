package me.jaehyeon.chapter6

import me.jaehyeon.sensor.SensorReading
import me.jaehyeon.sensor.SensorSource
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import java.time.Duration

/**
 * This Flink job demonstrates the use of a `KeyedProcessFunction` with processing time timers.
 *
 * The goal is to emit an alert if a sensor's temperature does not increase for one second
 * of processing time.
 *
 * The pipeline works as follows:
 * 1. **Source**: A stream of `SensorReading` events is generated. No watermarks are used,
 *    so all time-based operations default to processing time.
 * 2. **KeyBy**: The stream is partitioned by sensor ID.
 * 3. **Process**: A custom `TempIncreaseAlertFunction` is applied.
 *    - For each sensor, it checks if the temperature is increasing.
 *    - If the temperature increases, it sets a 1-second timer to check for another increase.
 *    - If the temperature decreases or stays the same, it cancels the existing timer.
 *    - If a timer fires, it means no temperature increase occurred within 1 second, and an alert is emitted.
 */
object ProcessFunctionTimers {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        // Source the sensor readings (using processing time, so no watermarks)
        val readings =
            env.fromSource(
                SensorSource(),
                WatermarkStrategy.noWatermarks(),
                "Sensor Source",
            )

        // Apply the KeyedProcessFunction
        val alerts =
            readings
                .keyBy { it.id }
                .process(TempIncreaseAlertFunction())

        // Print the alerts
        alerts.print()

        env.execute("Processing Time Timers Example")
    }
}

/**
 * A KeyedProcessFunction that emits an alert if a sensor's temperature
 * does not increase for 1 second of processing time.
 */
class TempIncreaseAlertFunction : KeyedProcessFunction<String, SensorReading, String>() {
    // State to store the last seen temperature for the current key
    private lateinit var lastTempState: ValueState<Double>

    // State to store the timestamp of the currently registered timer
    private lateinit var timerState: ValueState<Long>

    override fun open(parameters: Configuration) {
        // Initialize state descriptors
        val lastTempDescriptor = ValueStateDescriptor("lastTemp", Types.DOUBLE)
        lastTempState = runtimeContext.getState(lastTempDescriptor)

        val timerDescriptor = ValueStateDescriptor("timer", Types.LONG)
        timerState = runtimeContext.getState(timerDescriptor)
    }

    override fun processElement(
        value: SensorReading,
        ctx: Context,
        out: Collector<String>,
    ) {
        // Get the previous temperature and the current timer from state
        val prevTemp = lastTempState.value() ?: 0.0 // Default to 0.0 for the first element
        val timerTs = timerState.value() ?: 0L

        // Update the last temperature state with the current reading's temperature
        lastTempState.update(value.temperature)

        // Check if the temperature is increasing
        if (value.temperature > prevTemp) {
            // If it's increasing and no timer is currently set, register a new one
            // Note if it isn't increasing, timerStage gets clear in the else clause and timerTs gets 0L.
            if (timerTs == 0L) {
                val newTimer = ctx.timerService().currentProcessingTime() + 1000L
                ctx.timerService().registerProcessingTimeTimer(newTimer)
                // Store the new timer's timestamp in state
                timerState.update(newTimer)
            }
        } else {
            // If the temperature is not increasing, delete any existing timer
            if (timerTs != 0L) {
                ctx.timerService().deleteProcessingTimeTimer(timerTs)
                // Clear the timer state
                timerState.clear()
            }
        }
    }

    override fun onTimer(
        timestamp: Long,
        ctx: OnTimerContext,
        out: Collector<String>,
    ) {
        // When the timer fires, it means the temperature did not increase for 1 second.
        val alertMsg = "Temperature of sensor '${ctx.currentKey}' did not increase for 1 second."
        out.collect(alertMsg)

        // Clear the timer state so a new timer can be registered on the next increase.
        timerState.clear()
    }
}
