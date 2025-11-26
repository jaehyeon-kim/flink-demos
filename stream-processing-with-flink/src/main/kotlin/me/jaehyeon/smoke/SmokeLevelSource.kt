package me.jaehyeon.smoke

import org.apache.flink.api.connector.source.Boundedness
import org.apache.flink.api.connector.source.Source
import org.apache.flink.api.connector.source.SourceReader
import org.apache.flink.api.connector.source.SourceReaderContext
import org.apache.flink.api.connector.source.SplitEnumerator
import org.apache.flink.api.connector.source.SplitEnumeratorContext
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.util.InstantiationUtil

/**
 * A modern Flink Source that generates a random stream of SmokeLevel events.
 * This implementation uses the new Source API and is the recommended approach.
 */
class SmokeLevelSource : Source<SmokeLevel, SmokeLevelSplit, Unit> {
    override fun getBoundedness(): Boundedness = Boundedness.CONTINUOUS_UNBOUNDED

    override fun createReader(readerContext: SourceReaderContext): SourceReader<SmokeLevel, SmokeLevelSplit> =
        SmokeLevelSourceReader(readerContext)

    override fun createEnumerator(enumContext: SplitEnumeratorContext<SmokeLevelSplit>): SplitEnumerator<SmokeLevelSplit, Unit> =
        SimpleSplitEnumerator(enumContext)

    override fun restoreEnumerator(
        enumContext: SplitEnumeratorContext<SmokeLevelSplit>,
        checkpoint: Unit,
    ): SplitEnumerator<SmokeLevelSplit, Unit> = createEnumerator(enumContext)

    private class SimpleSplitEnumerator(
        private val context: SplitEnumeratorContext<SmokeLevelSplit>,
    ) : SplitEnumerator<SmokeLevelSplit, Unit> {
        override fun start() {}

        override fun addReader(subtaskId: Int) {
            // Assign a single dummy split to each reader.
            context.assignSplit(SmokeLevelSplit(), subtaskId)
        }

        override fun addSplitsBack(
            splits: MutableList<SmokeLevelSplit>,
            subtaskId: Int,
        ) {
            splits.forEach { context.assignSplit(it, subtaskId) }
        }

        override fun handleSplitRequest(
            subtaskId: Int,
            requesterHostname: String?,
        ) {}

        override fun snapshotState(checkpointId: Long) {}

        override fun close() {}
    }

    // --- Serializers ---
    override fun getSplitSerializer(): SimpleVersionedSerializer<SmokeLevelSplit> = SerializableSerializer()

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
