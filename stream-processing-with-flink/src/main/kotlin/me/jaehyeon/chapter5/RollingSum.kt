package me.jaehyeon.chapter5

import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object RollingSum {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        val inputStream =
            env.fromData(
                Tuple3(1, 2, 2),
                Tuple3(2, 3, 1),
                Tuple3(2, 2, 4),
                Tuple3(1, 5, 3),
            )

        val resultStream = inputStream.keyBy { it.f0 }.sum(1)

        resultStream.print()

        env.execute("Rolling Sum Example")
    }
}
