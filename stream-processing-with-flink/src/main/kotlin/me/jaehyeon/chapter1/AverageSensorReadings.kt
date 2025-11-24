package me.jaehyeon.chapter1

import me.jaehyeon.util.SensorReading
import me.jaehyeon.util.SensorSource
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.time.Duration

object AverageSensorReadings {
    @JvmStatic
    fun main(args: Array<String>) {
        // Set up the streaming execution environment
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        // Use the new Source API and assign watermarks
        val sensorData =
            env.fromSource(
                SensorSource(),
                WatermarkStrategy
                    .forBoundedOutOfOrderness<SensorReading>(Duration.ofSeconds(5))
                    .withTimestampAssigner { reading, _ ->
                        reading.timestamp
                    },
                "Sensor Source",
            )

        val avgTemp =
            sensorData
                .map { r ->
                    SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0))
                }.keyBy { it.id }
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(1)))
                .aggregate(TemperatureAggregator(), TemperatureAverager())

        avgTemp.print()

        env.execute("Compute average sensor temperature")
    }
}

/**
 * An AggregateFunction to incrementally compute the sum and count of temperatures.
 */
class TemperatureAggregator : AggregateFunction<SensorReading, Pair<Int, Double>, Double> {
    override fun createAccumulator(): Pair<Int, Double> = 0 to 0.0

    override fun add(
        value: SensorReading,
        accumulator: Pair<Int, Double>,
    ): Pair<Int, Double> = (accumulator.first + 1) to (accumulator.second + value.temperature)

    override fun getResult(accumulator: Pair<Int, Double>): Double =
        if (accumulator.first == 0) 0.0 else accumulator.second / accumulator.first

    override fun merge(
        a: Pair<Int, Double>,
        b: Pair<Int, Double>,
    ): Pair<Int, Double> = (a.first + b.first) to (a.second + b.second)
}

/**
 * A ProcessWindowFunction to format the final output SensorReading.
 */
class TemperatureAverager : ProcessWindowFunction<Double, SensorReading, String, TimeWindow>() {
    override fun process(
        key: String,
        context: ProcessWindowFunction<Double, SensorReading, String, TimeWindow>.Context,
        elements: Iterable<Double>,
        out: Collector<SensorReading>,
    ) {
        val avgTemp = elements.first()
        out.collect(SensorReading(key, context.window().end, avgTemp))
    }
}
