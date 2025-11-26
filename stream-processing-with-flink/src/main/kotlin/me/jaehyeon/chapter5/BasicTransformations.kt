package me.jaehyeon.chapter5

import me.jaehyeon.sensor.SensorReading
import me.jaehyeon.sensor.SensorSource
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import java.time.Duration

object BasicTransformations {
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

        val filteredSensors = readings.filter { r -> r.temperature >= 25 }
        val sensorIds = filteredSensors.map { r -> r.id }
        val splitIds =
            sensorIds
                .flatMap { id, out: Collector<String> ->
                    id.split("_").forEach { part ->
                        out.collect(part)
                    }
                }.returns(Types.STRING)

        splitIds.print()

        env.execute("Basic Transformations Example")
    }
}
