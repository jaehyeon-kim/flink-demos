package me.jaehyeon.chapter5

import me.jaehyeon.sensor.SensorReading
import me.jaehyeon.sensor.SensorSource
import me.jaehyeon.smoke.Alert
import me.jaehyeon.smoke.SmokeLevel
import me.jaehyeon.smoke.SmokeLevelSource
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.util.Collector
import java.time.Duration

/**
 * This Flink job demonstrates how to combine two different streams to perform more
 * complex logic, implementing a classic "Broadcast State" pattern.
 *
 * The goal is to issue a fire `Alert` only when two conditions are met: a high smoke
 * level is detected system-wide, AND a sensor's temperature reading is dangerously high.
 *
 * The pipeline works as follows:
 * 1. **Sensor Stream**: A high-volume, parallel stream of `SensorReading` events, keyed by sensor ID.
 * 2. **Smoke Stream**: A low-volume, non-parallel "control" stream of `SmokeLevel` events (`High` or `Low`).
 * 3. **Connect & Broadcast**: The smoke stream is broadcast to every parallel instance of the
 *    downstream operator. This ensures all tasks processing sensor readings know the current smoke level.
 * 4. **CoFlatMapFunction**: A custom function (`RaiseAlertFlatMap`) processes both streams.
 *    - It uses the smoke stream to update its internal state (the current smoke level).
 *    - It uses the sensor stream to check temperatures against the current smoke level state and
 *      emits an `Alert` if the conditions for a fire risk are met.
 * 5. **Sink**: Prints the resulting `Alert` stream to the console.
 */
object MultiStreamTransformations {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        val sensorReading =
            env.fromSource(
                SensorSource(),
                WatermarkStrategy
                    .forBoundedOutOfOrderness<SensorReading>(Duration.ofSeconds(5))
                    .withTimestampAssigner { reading, _ ->
                        reading.timestamp
                    },
                "Sensor Source",
            )

        val smokeReading =
            env
                .fromSource(
                    SmokeLevelSource(),
                    WatermarkStrategy.noWatermarks(),
                    "Smoke Level Source",
                ).setParallelism(1)

        val keyed = sensorReading.keyBy { it.id }
        val alerts =
            keyed
                .connect(smokeReading.broadcast())
                .flatMap(RaiseAlertFlatMap())

        alerts.print()

        env.execute("Multi-Stream Transformations Example")
    }

    class RaiseAlertFlatMap : CoFlatMapFunction<SensorReading, SmokeLevel, Alert> {
        var smokeLevel = SmokeLevel.Low

        override fun flatMap1(
            value: SensorReading,
            out: Collector<Alert>,
        ) {
            if (smokeLevel == SmokeLevel.High && value.temperature > 100) {
                out.collect(Alert(value.id, value.timestamp, "Risk of fire!"))
            }
        }

        override fun flatMap2(
            value: SmokeLevel,
            out: Collector<Alert>,
        ) {
            smokeLevel = value
        }
    }
}
