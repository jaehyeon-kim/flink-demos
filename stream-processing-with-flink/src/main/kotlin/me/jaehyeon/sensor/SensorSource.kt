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
     * A simple SplitEnumerator that assigns one SensorSplit to each parallel SourceReader.
     * It runs on the JobManager.
     */
    private class SimpleSplitEnumerator(
        private val context: SplitEnumeratorContext<SensorSplit>,
    ) : SplitEnumerator<SensorSplit, Unit> {
        override fun start() {
            // The start method is intentionally left empty. Splits are assigned
            // reactively when readers register via the addReader() method.
        }

        /**
         * This method is called by Flink whenever a new SourceReader subtask registers.
         * This is the correct and safe place to assign a split, as it avoids race
         * conditions where a split might be assigned before the reader is ready.
         *
         * @param subtaskId The ID of the reader subtask that has registered.
         */
        override fun addReader(subtaskId: Int) {
            // Create a new split for the registered reader and assign it.
            context.assignSplit(SensorSplit(subtaskId), subtaskId)
        }

        override fun addSplitsBack(
            splits: MutableList<SensorSplit>,
            subtaskId: Int,
        ) {
            // In case of a reader failure, Flink calls this to return the splits that
            // were assigned to the failed reader. We simply re-assign them.
            splits.forEach { context.assignSplit(it, subtaskId) }
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
