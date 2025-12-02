package me.jaehyeon.chapter7

import me.jaehyeon.sensor.SensorReading
import me.jaehyeon.sensor.SensorSource
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import java.time.Duration
import kotlin.math.abs

/**
 * This Flink job demonstrates a basic stateful transformation using a RichFlatMapFunction.
 *
 * It processes a stream of sensor readings and issues an alert if the temperature of a sensor
 * changes by more than a given threshold compared to its last reading. This is a common pattern
 * for anomaly or change detection. The state is managed per-key (per-sensor).
 *
 * This example pipeline:
 * 1. Ingests a stream of `SensorReading` events.
 * 2. Keys the stream by sensor ID, so that state is maintained independently for each sensor.
 * 3. Applies a `RichFlatMapFunction` (`TemperatureAlertFunction`) that:
 *    a. Uses `ValueState` to store the last temperature reading for the current sensor.
 *    b. Compares the current temperature with the last one.
 *    c. Emits an alert if the difference exceeds a defined threshold.
 *    d. Updates the state with the new temperature.
 * 4. Prints the resulting alert stream to the console.
 */
object KeyedStateFunction {
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

        // Key the stream by sensor ID and apply the stateful flatMap function.
        val alerts =
            readings
                .keyBy { it.id }
                .flatMap(TemperatureAlertFunction(1.7))

        // Print the alerts to the console.
        alerts.print()

        // Execute the application.
        env.execute("Generate Temperature Alerts")
    }
}

/**
 * A stateful FlatMap function that checks for large temperature changes.
 *
 * @param threshold The temperature difference that will trigger an alert.
 */
class TemperatureAlertFunction(
    private val threshold: Double,
) : RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>>() {
    // Declare the state handle. It will be initialized in the open() method.
    // This state will store the last temperature seen for the current key.
    private lateinit var lastTempState: ValueState<Double>

    /**
     * This is a Flink lifecycle method, called once per parallel instance when the function is initialized.
     * It's the ideal place to set up state handles.
     */
    override fun open(parameters: Configuration) {
        // Create a state descriptor. This defines the name ("lastTemp") and type (Double) of the state.
        val lastTempDescriptor = ValueStateDescriptor("lastTemp", Types.DOUBLE)
        // Get the state handle from the runtime context. Flink manages the actual state storage.
        lastTempState = runtimeContext.getState(lastTempDescriptor)
    }

    /**
     * This method is called for each element in the stream.
     */
    override fun flatMap(
        value: SensorReading,
        out: Collector<Tuple3<String, Double, Double>>,
    ) {
        // Retrieve the last temperature for the current key from the state.
        // If no value is set yet (e.g., first reading for this key), it defaults to 0.0.
        val lastTemp = lastTempState.value() ?: 0.0

        // Calculate the absolute difference between the current and last temperature.
        val tempDiff = abs(value.temperature - lastTemp)

        // Check if the difference is greater than the threshold.
        if (tempDiff > threshold) {
            // If it is, emit an alert tuple: (sensorId, currentTemperature, temperatureDifference).
            out.collect(Tuple3(value.id, value.temperature, tempDiff))
        }

        // Update the state with the current temperature for the next reading.
        lastTempState.update(value.temperature)
    }
}
