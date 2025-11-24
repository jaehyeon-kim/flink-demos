package me.jaehyeon.util

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
 * It acts as a factory for creating the SplitEnumerator and the SourceReader.
 */
class SensorSource : Source<SensorReading, SensorSplit, Unit> {
    override fun getBoundedness(): Boundedness = Boundedness.CONTINUOUS_UNBOUNDED

    override fun createReader(readerContext: SourceReaderContext): SourceReader<SensorReading, SensorSplit> =
        SensorSourceReader(readerContext)

    override fun createEnumerator(enumContext: SplitEnumeratorContext<SensorSplit>): SplitEnumerator<SensorSplit, Unit> =
        SimpleSplitEnumerator(enumContext)

    override fun restoreEnumerator(
        enumContext: SplitEnumeratorContext<SensorSplit>,
        checkpoint: Unit,
    ): SplitEnumerator<SensorSplit, Unit> = createEnumerator(enumContext)

    private class SimpleSplitEnumerator(
        private val context: SplitEnumeratorContext<SensorSplit>,
    ) : SplitEnumerator<SensorSplit, Unit> {
        override fun start() {
            // Do nothing here. We will assign splits when readers register.
        }

        override fun handleSplitRequest(
            subtaskId: Int,
            requesterHostname: String?,
        ) {
            // This source pushes splits, so we don't need to handle requests.
        }

        override fun addSplitsBack(
            splits: MutableList<SensorSplit>,
            subtaskId: Int,
        ) {
            // If a reader fails, re-assign its splits.
            splits.forEach { context.assignSplit(it, subtaskId) }
        }

        /**
         * This method is called when a SourceReader subtask registers.
         * This is the correct place to assign it an initial split.
         */
        override fun addReader(subtaskId: Int) {
            // Assign one split per reader.
            context.assignSplit(SensorSplit(subtaskId), subtaskId)
        }

        override fun snapshotState(checkpointId: Long) { /* Stateless */ }

        override fun close() { /* No-op */ }
    }

    override fun getSplitSerializer(): SimpleVersionedSerializer<SensorSplit> = SerializableSerializer()

    override fun getEnumeratorCheckpointSerializer(): SimpleVersionedSerializer<Unit> = UnitSerializer()

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

    private class UnitSerializer : SimpleVersionedSerializer<Unit> {
        override fun getVersion(): Int = 1

        override fun serialize(obj: Unit): ByteArray = ByteArray(0)

        override fun deserialize(
            version: Int,
            serialized: ByteArray,
        ) = Unit
    }
}
