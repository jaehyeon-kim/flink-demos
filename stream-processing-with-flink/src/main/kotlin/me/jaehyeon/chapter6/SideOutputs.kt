package me.jaehyeon.chapter6

import me.jaehyeon.sensor.SensorReading
import me.jaehyeon.sensor.SensorSource
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag
import java.time.Duration

/**
 * This Flink job demonstrates the use of Side Outputs.
 *
 * A ProcessFunction can produce multiple result streams from a single input stream. The primary
 * result stream is emitted via the Collector (`out`), while additional streams, called side
 * outputs, are emitted via the Context (`ctx`).
 *
 * This example pipeline:
 * 1. Ingests a stream of `SensorReading` events.
 * 2. Uses a `ProcessFunction` (`FreezingMonitor`) to monitor temperatures.
 * 3. All sensor readings are passed through to the main output stream.
 * 4. If a reading's temperature is below freezing (32.0 F), a warning message is emitted
 *    to a separate "freezing-alarms" side output stream.
 * 5. The job then prints both the main stream and the side output stream to the console.
 */
object SideOutputs {
    // Best practice: Define the OutputTag once and reuse it for both emitting and retrieving.
    // The anonymous inner class syntax `object : ... {}` is crucial to help Flink's
    // type system capture the generic type information (String) before it's erased.
    val freezingAlarmsTag: OutputTag<String> = object : OutputTag<String>("freezing-alarms") {}

    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.checkpointConfig.checkpointInterval = 10 * 1000
        env.config.autoWatermarkInterval = 1000L

        // Ingest the stream of sensor readings
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

        // Apply the ProcessFunction. This returns the MAIN output stream.
        val monitoredReadings = readings.process(FreezingMonitor())

        // Retrieve the SIDE output stream using the same OutputTag instance.
        val freezingAlarms = monitoredReadings.getSideOutput(freezingAlarmsTag)

        // Print the side output stream (freezing alarms only)
        freezingAlarms.print()

        // Print the main output stream (all readings)
        // Note: Flink may interleave the output from different print sinks.
        // Prefixing with a label could clarify which stream is which.
        readings.print()

        env.execute("Side Outputs Example Job")
    }
}

/**
 * A ProcessFunction that monitors sensor temperatures. It forwards all SensorReading objects
 * to its main output, and emits a String warning to a side output if the temperature is below freezing.
 */
class FreezingMonitor : ProcessFunction<SensorReading, SensorReading>() {
    override fun processElement(
        value: SensorReading,
        ctx: Context,
        out: Collector<SensorReading>,
    ) {
        // Check if the temperature is below freezing.
        if (value.temperature < 32.0) {
            // If it is, emit a warning String to the side output.
            // We use the context (`ctx`) to access the output method.
            ctx.output(
                SideOutputs.freezingAlarmsTag, // Use the shared OutputTag instance
                "Freezing Alarm for ${value.id} at ${value.temperature}",
            )
        }
        // Forward all sensor readings to the main output stream.
        out.collect(value)
    }
}
