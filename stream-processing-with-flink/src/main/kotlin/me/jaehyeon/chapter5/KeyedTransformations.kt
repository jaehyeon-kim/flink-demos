package me.jaehyeon.chapter5

import me.jaehyeon.sensor.SensorReading
import me.jaehyeon.sensor.SensorSource
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import java.time.Duration

/**
 * This Flink job demonstrates transformations on a `KeyedStream`.
 *
 * It showcases the `reduce` operator, a powerful tool for maintaining running aggregates
 * for each key in a stream.
 *
 * The pipeline is as follows:
 * 1. **Source**: Ingests a stream of `SensorReading` events.
 * 2. **KeyBy**: Partitions the stream by the `id` of each sensor. All subsequent
 *    operations will run independently for each sensor.
 * 3. **Reduce**: For each key, this operator maintains a running state of the `SensorReading`
 *    with the maximum temperature seen so far. For every new reading that arrives, it
 *    compares it to the current maximum and emits the new maximum downstream.
 * 4. **Sink**: Prints the continuous stream of running maximums for each sensor to the console.
 */
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
