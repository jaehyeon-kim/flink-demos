package me.jaehyeon.chapter8

import me.jaehyeon.connector.HttpSink
import me.jaehyeon.connector.HttpSource
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object CustomConnectors {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.parallelism = 2

        val httpSource =
            HttpSource(
                baseUrlPattern = "https://jsonplaceholder.typicode.com/posts/{id}",
                startId = 1,
                maxId = 100,
            )

        val sourceStream =
            env.fromSource(
                httpSource,
                WatermarkStrategy.noWatermarks(),
                "Cyclical HTTP Source",
            )

        val httpSink =
            HttpSink(
                url = "https://jsonplaceholder.typicode.com/posts",
                httpMethodName = "POST",
            )

        sourceStream.sinkTo(httpSink)

        env.execute("Custom HTTP Source and Sink Jobs")
    }
}
