package me.jaehyeon.chapter7

import me.jaehyeon.sensor.SensorReading
import me.jaehyeon.sensor.SensorSource
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.runtime.state.FunctionInitializationContext
import org.apache.flink.runtime.state.FunctionSnapshotContext
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import java.time.Duration

/**
 * This Flink job demonstrates the simultaneous use of Keyed State and Operator State
 * within a single function.
 *
 * This is a powerful combination for complex stateful logic. The function can maintain
 * state for each key it processes, while also maintaining separate state that is shared
 * across all keys within its own parallel instance.
 *
 * This example pipeline:
 * 1. Ingests a stream of `SensorReading` events.
 * 2. Keys the stream by sensor ID using `keyBy`. This is crucial for Keyed State to work.
 * 3. Applies a `FlatMapFunction` that implements `CheckpointedFunction`.
 * 4. The function maintains two counters:
 *    a. A per-sensor count using Keyed `ValueState`.
 *    b. A per-subtask total count using Operator `ListState`.
 * 5. The function outputs a tuple of `(sensorId, countForThatSensor, countForThatSubtask)`.
 */
object KeyedAndOperatorStateFunction {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.checkpointConfig.checkpointInterval = 10 * 1000
        env.config.autoWatermarkInterval = 1000L

        // Ingest the stream of sensor readings.
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

        val highTempCounts =
            readings
                .keyBy { it.id } // The stream is keyed by sensor ID.
                .flatMap(HighTempCounterWithKeyedAndOperatorState(10.0))

        highTempCounts.print()

        env.execute("High Temp Counter with Keyed and Operator State")
    }
}

/**
 * This function calculates two distinct counts using two different types of state:
 * 1. A count of high-temperature readings for each unique sensor ID (Keyed State).
 * 2. A total count of all high-temperature readings processed by this specific parallel
 *    instance/subtask (Operator State).
 */
class HighTempCounterWithKeyedAndOperatorState(
    private val threshold: Double,
) : FlatMapFunction<SensorReading, Triple<String, Long, Long>>,
    CheckpointedFunction {
    // Local, in-memory variable for the operator-level count.
    private var opHighTempCnt: Long = 0

    // Flink-managed state handles. Marked @Transient because they are initialized by Flink.
    @Transient
    private lateinit var keyedCntState: ValueState<Long>

    // For per-sensor counts.
    @Transient
    private lateinit var opCntState: ListState<Long> // For per-subtask counts.

    /**
     * The main processing logic, called for each element.
     */
    override fun flatMap(
        value: SensorReading,
        out: Collector<Triple<String, Long, Long>>,
    ) {
        if (value.temperature > threshold) {
            // --- Operator State Logic ---
            // Increment the local operator counter.
            opHighTempCnt++

            // --- Keyed State Logic ---
            // Retrieve the current count for this specific key (sensor ID).
            val currentKeyedCount = keyedCntState.value() ?: 0L
            val newKeyedCount = currentKeyedCount + 1
            // Update the keyed state with the new count. Flink ensures this only affects the current key.
            keyedCntState.update(newKeyedCount)

            // Emit the sensor ID, its specific count, and the subtask's total count.
            out.collect(Triple(value.id, newKeyedCount, opHighTempCnt))
        }
    }

    /**
     * Called on every checkpoint. Persists the OPERATOR state.
     * Flink automatically snapshots the KEYED state, so we don't need to handle it here.
     */
    override fun snapshotState(context: FunctionSnapshotContext) {
        opCntState.clear()
        opCntState.add(opHighTempCnt)
    }

    /**
     * Called once on initialization or recovery.
     * This is where we get handles for both Keyed and Operator state.
     */
    override fun initializeState(context: FunctionInitializationContext) {
        // --- Initialize Keyed State ---
        val keyCntDescriptor = ValueStateDescriptor("keyedCnt", Types.LONG)
        // Get state from the KeyedStateStore. Flink manages scoping this to the current key.
        keyedCntState = context.keyedStateStore.getState(keyCntDescriptor)

        // --- Initialize Operator State ---
        val opCntDescriptor = ListStateDescriptor("opCnt", Types.LONG)
        // Get state from the OperatorStateStore.
        opCntState = context.operatorStateStore.getListState(opCntDescriptor)

        // If restoring, initialize the local variable from the operator state.
        if (context.isRestored) {
            opHighTempCnt = opCntState.get().sum()
        }
    }
}
