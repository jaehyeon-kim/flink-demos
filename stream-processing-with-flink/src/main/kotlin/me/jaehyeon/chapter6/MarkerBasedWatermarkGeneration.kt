package me.jaehyeon.chapter6

import me.jaehyeon.sensor.SensorReading
import me.jaehyeon.sensor.SensorSource
import org.apache.flink.api.common.eventtime.Watermark
import org.apache.flink.api.common.eventtime.WatermarkGenerator
import org.apache.flink.api.common.eventtime.WatermarkOutput
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * This Flink job demonstrates a custom, event-driven watermark generation strategy.
 *
 * This pattern, formerly known as "punctuated", emits a watermark on-the-fly
 * whenever it sees a specific "marker" event in the stream, rather than on a
 * periodic interval.
 */
object MarkerBasedWatermarkGeneration {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        val readings =
            env.fromSource(
                SensorSource(),
                WatermarkStrategy
                    .forGenerator { ctx -> MarkerBasedWatermarkGenerator() }
                    .withTimestampAssigner { reading, _ -> reading.timestamp },
                "Sensor Source",
            )

        readings.print()
        env.execute("Marker-Based Watermark Generation")
    }
}

/**
 * A custom WatermarkGenerator that emits a new watermark every time it sees a
 * specific marker event (in this case, a reading from "sensor_1").
 */
class MarkerBasedWatermarkGenerator : WatermarkGenerator<SensorReading> {
    /**
     * This method is called for every event. We inspect the event and decide whether to emit a watermark.
     */
    override fun onEvent(
        event: SensorReading,
        eventTimestamp: Long,
        output: WatermarkOutput,
    ) {
        // Emit a new watermark if the event is from our marker, "sensor_1".
        if (event.id == "sensor_1") {
            output.emitWatermark(Watermark(eventTimestamp))
        }
    }

    /**
     * This method is called periodically. Since our logic is purely event-driven,
     * we don't need to do anything here.
     */
    override fun onPeriodicEmit(output: WatermarkOutput) {
        // This is not a periodic generator, so we do nothing here.
    }
}
