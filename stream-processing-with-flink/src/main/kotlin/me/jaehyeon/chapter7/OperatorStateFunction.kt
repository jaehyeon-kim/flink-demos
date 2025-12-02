package me.jaehyeon.chapter7

import me.jaehyeon.sensor.SensorReading
import me.jaehyeon.sensor.SensorSource
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.runtime.state.FunctionInitializationContext
import org.apache.flink.runtime.state.FunctionSnapshotContext
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import java.time.Duration

/**
 * This Flink job demonstrates the use of Operator State.
 *
 * Operator state is scoped to a parallel instance (subtask) of an operator. It is useful when
 * state is not related to a specific key but to the operator instance itself. A common use case
 * is managing offsets for a source connector.
 *
 * This example pipeline:
 * 1. Ingests a stream of `SensorReading` events.
 * 2. Applies a `RichFlatMapFunction` (`HighTempCounter`) that is NOT preceded by a `keyBy` call.
 * 3. The function implements `CheckpointedFunction` to manually manage its state.
 * 4. For each parallel instance, the function counts the number of high-temperature readings it has
 *    processed and stores that count in Flink's managed operator state.
 * 5. The function outputs a tuple of `(subtaskIndex, countForThatSubtask)`.
 */
object OperatorStateFunction {
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

        // The flatMap is NOT keyed, so state will be managed per operator instance.
        val highTempCounts = readings.flatMap(HighTempCounter(120.0))

        highTempCounts.print()

        env.execute("Count high temperatures with Operator State")
    }
}

/**
 * Counts high-temperature readings per parallel instance (subtask) using Operator State.
 * This function implements `CheckpointedFunction` to gain manual control over its state
 * during initialization and checkpointing.
 *
 * @param threshold The temperature threshold that readings must exceed to be counted.
 */
class HighTempCounter(
    private val threshold: Double,
) : RichFlatMapFunction<SensorReading, Pair<Int, Long>>(),
    CheckpointedFunction {
    // The index of the current parallel subtask, initialized in open().
    private var subtaskIdx = 0

    // A local, non-managed variable to hold the running count for the current subtask.
    // This variable is restored from Flink's state in initializeState().
    private var highTempCnt = 0L

    // The handle to Flink's managed operator state. This is the persistent, fault-tolerant
    // version of our counter. It is marked as @Transient because it is not serializable
    // and will be initialized by Flink in initializeState().
    @Transient
    private lateinit var opCntState: ListState<Long>

    /**
     * Flink lifecycle method, called once per parallel instance.
     * Used here to get the subtask index from the runtime context.
     */
    override fun open(parameters: org.apache.flink.configuration.Configuration) {
        subtaskIdx = runtimeContext.taskInfo.indexOfThisSubtask
    }

    /**
     * The main processing logic, called for each element.
     */
    override fun flatMap(
        value: SensorReading,
        out: Collector<Pair<Int, Long>>,
    ) {
        if (value.temperature > threshold) {
            // Increment the local, in-memory counter.
            highTempCnt++
            // Emit the subtask index and its current count.
            out.collect(Pair(subtaskIdx, highTempCnt))
        }
    }

    /**
     * Called by Flink automatically on every checkpoint.
     * This is our chance to move our local state into Flink's managed state for persistence.
     */
    override fun snapshotState(context: FunctionSnapshotContext) {
        // Clear any previous state in the handle.
        opCntState.clear()
        // Add the latest local count to the state handle. Flink will now checkpoint this value.
        opCntState.add(highTempCnt)
    }

    /**
     * Called by Flink when the function is first initialized or when recovering from a failure.
     * This is where we get our state handles and restore our local variables from managed state.
     */
    override fun initializeState(context: FunctionInitializationContext) {
        // Create a descriptor for the state, giving it a name and type.
        val opCntDescriptor = ListStateDescriptor("opCnt", Types.LONG)
        // Get the state handle for ListState from Flink's operator state store.
        opCntState = context.operatorStateStore.getListState(opCntDescriptor)

        // Check if we are recovering from a previous failure.
        if (context.isRestored) {
            // If so, restore the local counter by summing up the values in the state.
            // In this simple case, the list will only ever contain one element, but summing
            // is the correct way to handle rescaling from a different parallelism.
            highTempCnt = opCntState.get().sum()
        }
    }
}
