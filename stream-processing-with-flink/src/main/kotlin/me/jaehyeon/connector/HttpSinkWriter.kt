package me.jaehyeon.connector

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.request
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpMethod
import io.ktor.http.contentType
import io.ktor.http.isSuccess
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.runBlocking
import org.apache.flink.api.connector.sink2.SinkWriter
import org.slf4j.LoggerFactory

/**
 * The SinkWriter is the core logic of the sink. It runs on a TaskManager and
 * handles the actual writing of data to the external system.
 *
 * @param url The target HTTP endpoint.
 * @param httpMethodName The HTTP method name to use (e.g., POST, PUT).
 * @param subtaskId The parallel instance ID, used for logging.
 */
class HttpSinkWriter(
    private val url: String,
    httpMethodName: String, // Accept a String instead of HttpMethod
    private val subtaskId: Int,
) : SinkWriter<Post> {
    private val log = LoggerFactory.getLogger(HttpSinkWriter::class.java)
    private var httpClient: HttpClient

    // Parse the String into a real HttpMethod object here.
    private val httpMethod: HttpMethod = HttpMethod.parse(httpMethodName)

    init {
        log.info("Sink(Task $subtaskId) is initializing HTTP client for endpoint: $url")
        httpClient =
            HttpClient(CIO) {
                install(ContentNegotiation) {
                    json()
                }
                engine {
                    requestTimeout = 10_000 // Timeout for the entire request
                    maxConnectionsCount = 10 // Max connection count
                }
            }
    }

    override fun write(
        element: Post,
        context: SinkWriter.Context,
    ) {
        log.info("Sink(Task $subtaskId): Sending post with ID ${element.id}")
        runBlocking {
            try {
                val response: HttpResponse =
                    httpClient.request(url) {
                        // Use the parsed httpMethod object
                        method = httpMethod
                        contentType(ContentType.Application.Json)
                        setBody(element)
                    }
                if (!response.status.isSuccess()) {
                    log.error(
                        "Sink(Task $subtaskId): HTTP request failed for element ID ${element.id}. Status: ${response.status}. Body: ${response.bodyAsText()}",
                    )
                }
            } catch (e: Exception) {
                log.error("Sink(Task $subtaskId): Exception while sending element ID ${element.id}: ${e.message}")
            }
        }
    }

    override fun flush(endOfInput: Boolean) {
        log.info("Sink(Task {}): Flushing...", subtaskId)
    }

    override fun close() {
        log.info("Sink(Task {}): Closing HTTP client.", subtaskId)
        httpClient.close()
    }
}
