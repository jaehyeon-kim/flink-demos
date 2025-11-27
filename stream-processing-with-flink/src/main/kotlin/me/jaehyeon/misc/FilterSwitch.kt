package me.jaehyeon.misc

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.connector.datagen.source.DataGeneratorSource
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * A factory object for creating filter switch sources.
 * Encapsulating this logic keeps the main job definition clean and focused.
 */
object FilterSwitch {
    /**
     * Creates a stream of Tuple2<String, Long> filter commands.
     * Elements are emitted with a 1-second delay between them to simulate a real-world scenario.
     */
    fun createFilterSwitchSource(env: StreamExecutionEnvironment): DataStreamSource<Tuple2<String, Long>> {
        val filterCommands =
            listOf(
                Tuple2("sensor_2", 5 * 1000L),
                Tuple2("sensor_7", 6 * 1000L),
                Tuple2("sensor_2", 10 * 1000L),
            )

        val generatorSource =
            DataGeneratorSource(
                { index -> filterCommands[index.toInt()] },
                filterCommands.size.toLong(),
                RateLimiterStrategy.perSecond(1.0),
                Types.TUPLE(Types.STRING, Types.LONG),
            )

        return env.fromSource(
            generatorSource,
            WatermarkStrategy.noWatermarks(),
            "Filter Switch Generator",
        )
    }
}
