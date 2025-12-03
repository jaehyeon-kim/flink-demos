package me.jaehyeon.connector

import org.apache.flink.api.connector.source.Boundedness
import org.apache.flink.api.connector.source.Source
import org.apache.flink.api.connector.source.SourceReader
import org.apache.flink.api.connector.source.SourceReaderContext
import org.apache.flink.api.connector.source.SplitEnumerator
import org.apache.flink.api.connector.source.SplitEnumeratorContext
import org.apache.flink.core.io.SimpleVersionedSerializer
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

/**
 * The main entry point for the custom HTTP Source. It provides Flink with the
 * necessary components and serializers to run the source.
 *
 * @param baseUrlPattern A URL string containing "{id}" as a placeholder.
 * @param startId The first ID in the cycle (inclusive).
 * @param maxId The last ID in the cycle (inclusive).
 */
class HttpSource(
    private val baseUrlPattern: String,
    private val startId: Long,
    private val maxId: Long,
) : Source<Post, HttpSplit, Long> {
    override fun getBoundedness(): Boundedness = Boundedness.CONTINUOUS_UNBOUNDED

    override fun createReader(readerContext: SourceReaderContext): SourceReader<Post, HttpSplit> = HttpSourceReader(readerContext)

    override fun createEnumerator(enumContext: SplitEnumeratorContext<HttpSplit>): SplitEnumerator<HttpSplit, Long> =
        HttpSplitEnumerator(enumContext, baseUrlPattern, startId, maxId, null)

    override fun restoreEnumerator(
        enumContext: SplitEnumeratorContext<HttpSplit>,
        checkpoint: Long,
    ): SplitEnumerator<HttpSplit, Long> = HttpSplitEnumerator(enumContext, baseUrlPattern, startId, maxId, checkpoint)

    // --- Serializers ---
    override fun getSplitSerializer(): SimpleVersionedSerializer<HttpSplit> = HttpSplitSerializer()

    override fun getEnumeratorCheckpointSerializer(): SimpleVersionedSerializer<Long> =
        object : SimpleVersionedSerializer<Long> {
            override fun getVersion(): Int = 1

            override fun serialize(checkpoint: Long): ByteArray =
                ByteArrayOutputStream().use { baos ->
                    DataOutputStream(baos).use { out ->
                        out.writeLong(checkpoint)
                        baos.toByteArray()
                    }
                }

            override fun deserialize(
                version: Int,
                serialized: ByteArray,
            ): Long =
                ByteArrayInputStream(serialized).use { bais ->
                    DataInputStream(bais).use { inp -> inp.readLong() }
                }
        }
}
