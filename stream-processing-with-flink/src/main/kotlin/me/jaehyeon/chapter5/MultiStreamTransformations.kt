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
