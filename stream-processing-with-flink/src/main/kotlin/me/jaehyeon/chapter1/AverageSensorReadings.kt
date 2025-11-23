package me.jaehyeon.chapter1

import me.jaehyeon.util.SensorReading
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object AverageSensorReadings {
    @JvmStatic
    fun main(args: Array<String>) {
        // Set up the streaming execution environment
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        // Create a stream of sensor readings for demonstration
        val sensorData =
            env.fromElements(
                SensorReading("sensor_1", 1547718199, 35.8),
                SensorReading("sensor_6", 1547718201, 15.4),
                SensorReading("sensor_7", 1547718202, 6.7),
            )

        // Print the stream to the console
        sensorData.print()

        // Execute the job
        env.execute("My First Flink Job")
    }
}
