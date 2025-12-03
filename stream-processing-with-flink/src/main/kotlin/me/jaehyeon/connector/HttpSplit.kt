package me.jaehyeon.connector

import org.apache.flink.api.connector.source.SourceSplit
import org.apache.flink.core.io.SimpleVersionedSerializer
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException

/**
 * Represents one unit of work: a single URL to be fetched by a SourceReader.
 */
data class HttpSplit(
    val url: String,
) : SourceSplit {
    override fun splitId(): String = url
}

/**
 * Serializer for sending HttpSplit objects from the JobManager (Enumerator)
 * to the TaskManagers (Readers).
 */
class HttpSplitSerializer : SimpleVersionedSerializer<HttpSplit> {
    companion object {
        private const val VERSION = 1
    }

    override fun getVersion(): Int = VERSION

    override fun serialize(split: HttpSplit): ByteArray =
        ByteArrayOutputStream().use { baos ->
            DataOutputStream(baos).use { out ->
                out.writeUTF(split.url)
                baos.toByteArray()
            }
        }

    override fun deserialize(
        version: Int,
        serialized: ByteArray,
    ): HttpSplit {
        if (version != VERSION) throw IOException("Unknown version: $version")
        return ByteArrayInputStream(serialized).use { bais ->
            DataInputStream(bais).use { inp ->
                HttpSplit(inp.readUTF())
            }
        }
    }
}
