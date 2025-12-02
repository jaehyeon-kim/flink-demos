package me.jaehyeon.chapter7

import me.jaehyeon.misc.ControlStreamGenerator
import me.jaehyeon.sensor.SensorReading
import me.jaehyeon.sensor.SensorSource
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import java.time.Duration
import kotlin.math.abs

/**
 * This Flink job demonstrates the use of Broadcast State to dynamically update operator logic.
 *
 * Broadcast State allows a "control" stream to send data to all parallel instances of an operator,
 * making it ideal for distributing configuration, rules, or patterns that should apply globally.
 *
 * This example pipeline:
 * 1. Ingests a main stream of `SensorReading` events.
 * 2. Ingests a separate, low-volume "control" stream of `(SensorID, Threshold)` tuples.
 * 3. The control stream is broadcast to all parallel instances of a custom processing function.
 * 4. A `KeyedBroadcastProcessFunction` connects the main keyed sensor stream with the broadcast
 *    control stream.
 * 5. This function stores the incoming thresholds in its broadcast state. When processing sensor
 *    readings, it uses the latest broadcasted threshold for that sensor to decide whether to
 *    issue a temperature alert.
 * 6. This allows thresholds to be updated on-the-fly without restarting the job.
 */
object BroadcastStateFunction {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.checkpointConfig.checkpointInterval = 10 * 1000
        env.config.autoWatermarkInterval = 1000L

        // Ingest the main stream of sensor readings.
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

        // Define the data for the control stream of threshold updates.
        val thresholdUpdates =
            listOf(
                Tuple2("sensor_1", 5.0),
                Tuple2("sensor_2", 0.9),
                Tuple2("sensor_3", 0.5),
                Tuple2("sensor_1", 1.2), // update threshold for sensor_1
                Tuple2("sensor_3", 0.0), // disable threshold for sensor_3
            )

        // Create the control stream using the generic generator.
        val sensorThresholds =
            ControlStreamGenerator
                .createSource(
                    env,
                    "Sensor Threshold Generator",
                    thresholdUpdates,
                    Types.TUPLE(Types.STRING, Types.DOUBLE),
                )

        // Define the descriptor for the broadcast state. This specifies the key/value types.
        val broadcastStateDescriptor = MapStateDescriptor("thresholds", Types.STRING, Types.DOUBLE)
        // Broadcast the control stream. All downstream operators will receive all elements.
        val broadcastedThresholds = sensorThresholds.broadcast(broadcastStateDescriptor)

        val alerts =
            readings
                .keyBy { it.id }
                // Connect the main keyed stream with the broadcasted control stream.
                .connect(broadcastedThresholds)
                // Apply the function that uses both streams.
                .process(UpdatableTemperatureAlertFunction())

        alerts.print()

        env.execute("Generate Temperature Alerts using Broadcast State")
    }
}

/**
 * A KeyedBroadcastProcessFunction that generates temperature alerts based on dynamically
 * updatable thresholds.
 */
class UpdatableTemperatureAlertFunction :
    KeyedBroadcastProcessFunction<String, SensorReading, Tuple2<String, Double>, Tuple3<String, Double, Double>>() {
    // Keyed state, storing the last temperature for the current sensor key.
    private lateinit var lastTempState: ValueState<Double>

    // A descriptor for the broadcast state. It must match the one used in the .broadcast() call.
    // It's defined here again to access the state in the process methods.
    private val thresholdStateDescriptor = MapStateDescriptor("thresholds", Types.STRING, Types.DOUBLE)

    override fun open(parameters: Configuration) {
        // Initialize the keyed state for last temperature.
        val lastTempDescriptor = ValueStateDescriptor("lastTemp", Types.DOUBLE)
        lastTempState = runtimeContext.getState(lastTempDescriptor)
    }

    /**
     * This method is called for each element in the BROADCAST stream (threshold updates).
     * It updates the shared broadcast state, which is visible to all parallel instances.
     */
    override fun processBroadcastElement(
        value: Tuple2<String, Double>, // A (sensorId, newThreshold) tuple
        ctx: Context,
        out: Collector<Tuple3<String, Double, Double>>,
    ) {
        // Get the handle to the broadcast state (it is writable here).
        val thresholds = ctx.getBroadcastState(thresholdStateDescriptor)

        if (value.f1 != 0.0) {
            // If the new threshold is not zero, add or update it in the state.
            println("UPDATING THRESHOLD for ${value.f0} to ${value.f1}")
            thresholds.put(value.f0, value.f1)
        } else {
            // If the threshold is zero, we treat it as a signal to remove the sensor's threshold.
            println("REMOVING THRESHOLD for ${value.f0}")
            thresholds.remove(value.f0)
        }
    }

    /**
     * This method is called for each element in the main KEYED stream (sensor readings).
     * It can read from the broadcast state but cannot modify it.
     */
    override fun processElement(
        value: SensorReading, // The current sensor reading
        ctx: ReadOnlyContext,
        out: Collector<Tuple3<String, Double, Double>>,
    ) {
        // Get a read-only handle to the broadcast state.
        val thresholds = ctx.getBroadcastState(thresholdStateDescriptor)

        // Check if a threshold is configured for the current sensor ID.
        if (thresholds.contains(value.id)) {
            val sensorThreshold = thresholds.get(value.id)

            // Retrieve this key's last temperature from its private keyed state.
            val lastTemp = lastTempState.value() ?: 0.0
            val tempDiff = abs(value.temperature - lastTemp)

            // If the temperature jump exceeds the dynamically configured threshold, emit an alert.
            if (tempDiff > sensorThreshold) {
                out.collect(Tuple3(value.id, value.temperature, tempDiff))
            }
        }

        // Always update the keyed state with the latest temperature for this sensor.
        lastTempState.update(value.temperature)
    }
}
