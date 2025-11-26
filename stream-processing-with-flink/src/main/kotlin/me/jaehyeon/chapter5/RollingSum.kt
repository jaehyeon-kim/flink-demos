package me.jaehyeon.chapter5

import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * This Flink job demonstrates how to compute a "rolling sum" using the `.sum()`
 * aggregation function on a KeyedStream.
 *
 * A rolling sum is a continuous aggregation that is updated for every input event,
 * as opposed to a windowed aggregation which only emits a result at the end of a window.
 *
 * The pipeline works as follows:
 * 1. **Source**: A simple, static stream of `Tuple3` is created, where the first
 *    element is the key and the second is the value to be summed.
 * 2. **KeyBy**: The stream is partitioned by the first field (`f0`) using a type-safe lambda.
 * 3. **Sum**: The `.sum(1)` operator maintains a running sum of the second field (at index 1)
 *    for each key. It is a concise way to perform this specific aggregation.
 * 4. **Sink**: The resulting stream of continuous, updated sums is printed to the console.
 *
 * **Note on Best Practices:** While `.sum()` is convenient for Tuples, the modern,
 * recommended approach for most streaming applications is to use the more flexible and
 * fully type-safe `.reduce()` operator, especially when working with custom data classes.
 */
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
