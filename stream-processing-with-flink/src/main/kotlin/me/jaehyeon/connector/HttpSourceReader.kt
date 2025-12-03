package me.jaehyeon.connector

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.get
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.runBlocking
import org.apache.flink.api.connector.source.ReaderOutput
import org.apache.flink.api.connector.source.SourceReader
import org.apache.flink.api.connector.source.SourceReaderContext
import org.apache.flink.core.io.InputStatus
import org.slf4j.LoggerFactory
import java.util.LinkedList
import java.util.concurrent.CompletableFuture

/**
 * The worker that executes on a TaskManager. It requests splits, fetches data over
 * HTTP for each split, and emits the parsed records into the Flink stream.
 */
class HttpSourceReader(
    private val context: SourceReaderContext,
) : SourceReader<Post, HttpSplit> {
    private val log = LoggerFactory.getLogger(HttpSourceReader::class.java)

    @Transient
    private lateinit var httpClient: HttpClient
    private val assignedSplits = LinkedList<HttpSplit>()
    private var noMoreSplits = false

    override fun start() {
        httpClient =
            HttpClient(CIO) {
                install(ContentNegotiation) { json() }
            }
    }

    override fun pollNext(output: ReaderOutput<Post>): InputStatus {
        val split = assignedSplits.poll()
        if (split != null) {
            // We have a split, so process it.
            runBlocking {
                try {
                    val post = httpClient.get(split.url).body<Post>()
                    log.info("Fetched post with ID ${post.id}")
                    output.collect(post)
                } catch (e: Exception) {
                    log.error("Failed to fetch or parse URL ${split.url}: ${e.message}")
                }
            }
            // We successfully processed an element and might have more in the queue.
            return InputStatus.MORE_AVAILABLE
        }

        // We have no splits to process. We must request one from the enumerator.
        context.sendSplitRequest()

        // We have no data available AT THIS MOMENT, but have requested more.
        return InputStatus.NOTHING_AVAILABLE
    }

    /**
     * A new method required by the modern SourceReader interface.
     * It returns a future that completes when the source is ready to be polled.
     * For a synchronous source like this one, we are always ready.
     */
    override fun isAvailable(): CompletableFuture<Void> {
        // This source is synchronous and does its blocking I/O in pollNext,
        // so it's always considered "available" to be called.
        return CompletableFuture.completedFuture(null)
    }

    override fun snapshotState(checkpointId: Long): MutableList<HttpSplit> = assignedSplits

    override fun addSplits(splits: MutableList<HttpSplit>) {
        assignedSplits.addAll(splits)
    }

    override fun notifyNoMoreSplits() {
        noMoreSplits = true
    }

    override fun close() {
        if (::httpClient.isInitialized) {
            httpClient.close()
        }
    }
}
