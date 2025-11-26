package me.jaehyeon.chapter5

import me.jaehyeon.sensor.SensorReading
import me.jaehyeon.sensor.SensorSource
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import java.time.Duration

object KeyedTransformations {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

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

        val keyed = readings.keyBy { it.id }
        val maxTempPerSensor =
            keyed.reduce { r1, r2 ->
                if (r1.temperature > r2.temperature) r1 else r2
            }

        maxTempPerSensor.print()

        env.execute("Keyed Transformations Example")
    }
}
