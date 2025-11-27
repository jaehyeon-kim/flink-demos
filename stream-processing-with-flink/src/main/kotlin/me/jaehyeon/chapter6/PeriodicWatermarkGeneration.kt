package me.jaehyeon.chapter6

import me.jaehyeon.sensor.SensorReading
import me.jaehyeon.sensor.SensorSource
import org.apache.flink.api.common.eventtime.*
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import java.time.Duration

/**
 * This Flink job demonstrates how to create a custom "periodic" watermark generator.
 *
 * A periodic generator is called at a regular interval (defined by autoWatermarkInterval)
 * to emit a new watermark. This is the most common custom pattern and is similar to
 * Flink's built-in bounded-out-of-orderness strategy.
 */
object PeriodicWatermarkGeneration {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        // Configure the watermark emission interval to 1 second.
        env.config.autoWatermarkInterval = 1000L

        val readings =
            env.fromSource(
                SensorSource(),
                // Use a WatermarkStrategy with our custom periodic generator.
                WatermarkStrategy
                    .forGenerator { ctx -> PeriodicWatermarkGenerator(Duration.ofSeconds(5)) }
                    .withTimestampAssigner { reading, _ -> reading.timestamp },
                "Sensor Source",
            )

        readings.print()
        env.execute("Periodic Watermark Generation")
    }
}

/**
 * A custom WatermarkGenerator that implements a periodic, bounded-out-of-orderness strategy.
 * It tracks the highest timestamp seen so far and emits a watermark that is `maxOutOfOrderness`
 * behind that highest timestamp.
 *
 * @param maxOutOfOrderness The maximum delay for which out-of-order events are tolerated.
 */
class PeriodicWatermarkGenerator(
    private val maxOutOfOrderness: Duration,
) : WatermarkGenerator<SensorReading> {
    // The highest timestamp encountered so far.
    private var maxTimestamp = Long.MIN_VALUE + maxOutOfOrderness.toMillis() + 1

    /**
     * This method is called for every event. We use it to track the highest timestamp.
     */
    override fun onEvent(
        event: SensorReading,
        eventTimestamp: Long,
        output: WatermarkOutput,
    ) {
        maxTimestamp = maxOf(maxTimestamp, eventTimestamp)
    }

    /**
     * This method is called periodically by Flink. We use it to emit a new watermark
     * based on the highest timestamp we've seen.
     */
    override fun onPeriodicEmit(output: WatermarkOutput) {
        val watermarkTimestamp = maxTimestamp - maxOutOfOrderness.toMillis()
        output.emitWatermark(Watermark(watermarkTimestamp))
    }
}
