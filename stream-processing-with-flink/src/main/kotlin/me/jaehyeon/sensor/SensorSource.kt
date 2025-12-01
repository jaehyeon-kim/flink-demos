package me.jaehyeon.sensor

import org.apache.flink.api.connector.source.Boundedness
import org.apache.flink.api.connector.source.Source
import org.apache.flink.api.connector.source.SourceReader
import org.apache.flink.api.connector.source.SourceReaderContext
import org.apache.flink.api.connector.source.SplitEnumerator
import org.apache.flink.api.connector.source.SplitEnumeratorContext
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.util.InstantiationUtil

/**
 * The main SensorSource class that implements the new Flink Source API.
 *
 * This class acts as the entry point and a factory for Flink. It is responsible for:
 * 1. Declaring the boundedness of the source (it is continuous and unbounded).
 * 2. Creating the `SplitEnumerator`, which runs on the JobManager to coordinate and assign work.
 * 3. Creating the `SourceReader`, which runs on the TaskManagers to generate data.
 * 4. Providing serializers for the source's splits and checkpoints.
 */
class SensorSource : Source<SensorReading, SensorSplit, Unit> {
    override fun getBoundedness(): Boundedness = Boundedness.CONTINUOUS_UNBOUNDED

    override fun createReader(readerContext: SourceReaderContext): SourceReader<SensorReading, SensorSplit> {
        // Factory method to create the reader on the TaskManager.
        return SensorSourceReader(readerContext)
    }

    override fun createEnumerator(enumContext: SplitEnumeratorContext<SensorSplit>): SplitEnumerator<SensorSplit, Unit> {
        // Factory method to create the enumerator on the JobManager.
        return SimpleSplitEnumerator(enumContext)
    }

    override fun restoreEnumerator(
        enumContext: SplitEnumeratorContext<SensorSplit>,
        checkpoint: Unit,
    ): SplitEnumerator<SensorSplit, Unit> {
        // This source is stateless, so restoring is the same as creating a new enumerator.
        return createEnumerator(enumContext)
    }

    /**
     * A SplitEnumerator that handles task failures and restarts correctly.
     * It assigns one SensorSplit to each parallel SourceReader.
     * It runs on the JobManager.
     */
    private class SimpleSplitEnumerator(
        private val context: SplitEnumeratorContext<SensorSplit>,
    ) : SplitEnumerator<SensorSplit, Unit> {
        // Store splits that are waiting to be reassigned after a failure.
        private val pendingSplits = mutableMapOf<Int, MutableList<SensorSplit>>()

        override fun start() {
            // The start method is intentionally left empty. Splits are assigned
            // reactively when readers register.
        }

        override fun addReader(subtaskId: Int) {
            // This is the safe place to assign splits. Check if there are pending
            // splits for this subtask from a previous failure.
            val pending = pendingSplits.remove(subtaskId)
            if (pending != null && pending.isNotEmpty()) {
                // If splits were pending, re-assign them.
                context.assignSplits(
                    org.apache.flink.api.connector.source
                        .SplitsAssignment(mapOf(subtaskId to pending)),
                )
            } else {
                // Otherwise, assign a brand-new split for this reader.
                context.assignSplit(SensorSplit(subtaskId), subtaskId)
            }
        }

        override fun addSplitsBack(
            splits: MutableList<SensorSplit>,
            subtaskId: Int,
        ) {
            // A reader has failed. Instead of re-assigning its splits immediately,
            // which causes a race condition, we store them in our pending map.
            // They will be assigned later when the new reader registers via addReader().
            pendingSplits.computeIfAbsent(subtaskId) { mutableListOf() }.addAll(splits)
        }

        override fun handleSplitRequest(
            subtaskId: Int,
            requesterHostname: String?,
        ) {
            // This source actively pushes splits, so we don't need to handle pull requests.
        }

        override fun snapshotState(checkpointId: Long) {
            // This enumerator is stateless, so there's nothing to checkpoint.
        }

        override fun close() {
            // No resources to clean up.
        }
    }

    // --- Serializers ---

    override fun getSplitSerializer(): SimpleVersionedSerializer<SensorSplit> = SerializableSerializer()

    override fun getEnumeratorCheckpointSerializer(): SimpleVersionedSerializer<Unit> = UnitSerializer()

    /** A generic serializer for any class that implements java.io.Serializable. */
    private class SerializableSerializer<T : java.io.Serializable> : SimpleVersionedSerializer<T> {
        override fun getVersion(): Int = 1

        override fun serialize(obj: T): ByteArray = InstantiationUtil.serializeObject(obj)

        override fun deserialize(
            version: Int,
            serialized: ByteArray,
        ): T {
            @Suppress("UNCHECKED_CAST")
            return InstantiationUtil.deserializeObject(serialized, this.javaClass.classLoader) as T
        }
    }

    /** A specific serializer for Kotlin's Unit type, as it is not Serializable. */
    private class UnitSerializer : SimpleVersionedSerializer<Unit> {
        override fun getVersion(): Int = 1

        override fun serialize(obj: Unit): ByteArray = ByteArray(0)

        override fun deserialize(
            version: Int,
            serialized: ByteArray,
        ) = Unit
    }
}
