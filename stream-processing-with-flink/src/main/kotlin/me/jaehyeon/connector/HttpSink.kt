package me.jaehyeon.connector

import org.apache.flink.api.connector.sink2.Sink
import org.apache.flink.api.connector.sink2.SinkWriter
import org.apache.flink.api.connector.sink2.WriterInitContext

/**
 * The Sink class is the main entry point and a factory for the SinkWriter.
 *
 * @param url The target HTTP endpoint for all records.
 * @param httpMethodName The HTTP method name to use for the requests.
 */
class HttpSink(
    private val url: String,
    private val httpMethodName: String,
) : Sink<Post> {
    @Deprecated("Overrides deprecated member in superclass.")
    override fun createWriter(context: Sink.InitContext): SinkWriter<Post> {
        val subtaskId =
            if (context is WriterInitContext) {
                // Modern, warning-free path
                context.subtaskId
            } else {
                // Fallback path with targeted warning suppression
                @Suppress("DEPRECATION")
                context.getSubtaskId()
            }

        return HttpSinkWriter(
            url,
            httpMethodName,
            subtaskId,
        )
    }
}
