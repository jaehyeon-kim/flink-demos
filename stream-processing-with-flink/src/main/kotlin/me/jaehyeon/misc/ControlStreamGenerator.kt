package me.jaehyeon.misc

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy
import org.apache.flink.connector.datagen.source.DataGeneratorSource
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * A generic data generator for creating mock control streams in Flink.
 * This object provides a single, reusable method to generate a DataStreamSource
 * from a provided list of data, emitting one element per second.
 */
object ControlStreamGenerator {
    /**
     * Creates a Flink DataStreamSource from a list of elements.
     *
     * @param T The type of elements in the stream.
     * @param env The Flink StreamExecutionEnvironment.
     * @param sourceName A descriptive name for the Flink source.
     * @param data The list of data to be emitted by the source.
     * @param typeInfo The Flink TypeInformation for the data type T.
     * @return A DataStreamSource that will emit the elements from the data list.
     */
    fun <T> createSource(
        env: StreamExecutionEnvironment,
        sourceName: String,
        data: List<T>,
        typeInfo: TypeInformation<T>,
    ): DataStreamSource<T> {
        val generatorSource =
            DataGeneratorSource(
                { index -> data[index.toInt()] },
                data.size.toLong(),
                RateLimiterStrategy.perSecond(1.0),
                typeInfo,
            )

        return env.fromSource(
            generatorSource,
            WatermarkStrategy.noWatermarks(),
            sourceName,
        )
    }
}
