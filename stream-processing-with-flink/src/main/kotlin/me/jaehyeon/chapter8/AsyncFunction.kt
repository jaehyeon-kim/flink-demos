package me.jaehyeon.chapter8

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.get
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.future.future
import kotlinx.serialization.Serializable
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.datagen.source.DataGeneratorSource
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction
import java.util.Collections
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import java.io.Serializable as JavaSerializable

/**
 * This Flink job demonstrates how to use the Asynchronous I/O API to enrich a data stream.
 *
 * Asynchronous I/O is a crucial pattern for interacting with external systems (like databases or
 * REST APIs) without blocking the main stream processing threads, thus maintaining high throughput.
 *
 * This example pipeline:
 * 1. Generates a stream of random Long numbers, simulating post IDs.
 * 2. Uses `AsyncDataStream.unorderedWait` to apply a custom `RichAsyncFunction`.
 * 3. The `EnrichPost` function makes an asynchronous HTTP GET request to an external API
 *    for each post ID to fetch the full post details.
 * 4. The enriched `Post` objects are then printed to the console.
 */
object AsyncFunction {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        // 1. Create a source stream of random post IDs.
        // DataGeneratorSource is a utility source for creating simple, parallel data streams.
        val postIdStream =
            env.fromSource(
                DataGeneratorSource(
                    { _ -> Random.nextLong(1, 100) }, // Generator function
                    Long.MAX_VALUE, // Number of records to generate
                    RateLimiterStrategy.perSecond(5.0), // Rate of generation
                    Types.LONG, // The type of data produced
                ),
                WatermarkStrategy.noWatermarks(),
                "Random Post IDs",
            )

        // 2. Apply the asynchronous enrichment operation.
        // `unorderedWait` processes records concurrently and emits them as they complete,
        // potentially changing the stream's order. This offers the best performance.
        val enrichedPosts =
            AsyncDataStream
                .unorderedWait(
                    postIdStream, // The input stream
                    EnrichPost("https://jsonplaceholder.typicode.com/posts"), // The async function
                    2000, // Timeout: max time for a request to complete
                    TimeUnit.MILLISECONDS,
                    100, // Capacity: max number of async requests in flight at once
                )

        // 3. Print the resulting stream of enriched Post objects to standard out.
        enrichedPosts.print()

        // 4. Execute the Flink job.
        env.execute("Asynchronous Data Enrichment Job")
    }
}

/**
 * Data class representing a Post, fetched from the external API.
 *
 * It implements two types of Serializable:
 * - `@Serializable`: From `kotlinx.serialization`, used by Ktor to deserialize JSON into this object.
 * - `JavaSerializable`: Standard Java serialization, required by Flink to send objects between TaskManagers.
 *   An alias is used to avoid a name clash with kotlinx.serialization.Serializable.
 */
@Serializable
data class Post(
    val userId: Int,
    val id: Int,
    val title: String,
    val body: String,
) : JavaSerializable

/**
 * Implements the asynchronous enrichment logic.
 *
 * This `RichAsyncFunction` takes a post ID (`Long`) as input and outputs a fully enriched `Post` object.
 * It manages its own HTTP client and coroutine scope for making non-blocking API calls.
 *
 * @param requestUrl The base URL of the REST API to query.
 */
class EnrichPost(
    private val requestUrl: String,
) : RichAsyncFunction<Long, Post>() {
    // @Transient fields are not serialized by Flink during checkpointing.
    // They are runtime-only resources initialized in the open() method.
    @Transient
    private lateinit var httpClient: HttpClient

    @Transient
    private lateinit var scope: CoroutineScope

    /**
     * Initialization method for the function. Called once per parallel instance.
     * This is the ideal place to set up expensive resources like database connections or HTTP clients.
     */
    override fun open(parameters: Configuration?) {
        super.open(parameters)
        httpClient =
            HttpClient(CIO) {
                // Install the Ktor ContentNegotiation plugin to handle JSON serialization.
                install(ContentNegotiation) {
                    json() // Use kotlinx.serialization for JSON.
                }
            }
        // A dedicated CoroutineScope is created for managing the lifecycle of async operations.
        // Dispatchers.IO is used for blocking I/O calls, and SupervisorJob prevents one failure from canceling all others.
        scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    }

    /**
     * The core async logic, called for each record in the stream.
     * This method launches a coroutine to perform the HTTP request without blocking.
     *
     * @param input The incoming element from the stream (a post ID).
     * @param resultFuture A future that must be completed with the result of the async operation.
     */
    override fun asyncInvoke(
        input: Long,
        resultFuture: ResultFuture<Post>,
    ) {
        // Launch a new coroutine that will complete the ResultFuture.
        scope.future {
            try {
                // Make the asynchronous GET request.
                val response = httpClient.get("$requestUrl/$input")
                // Deserialize the JSON response body into a Post object.
                val post = response.body<Post>()
                // On success, complete the future with the result.
                resultFuture.complete(Collections.singleton(post))
            } catch (e: Exception) {
                // On failure, complete the future with an exception. This will cause the job to fail by default.
                resultFuture.completeExceptionally(e)
            }
        }
    }

    /**
     * Cleanup method for the function. Called once per parallel instance when the job is closing.
     * This is where resources should be released.
     */
    override fun close() {
        super.close()
        // Close the HTTP client to release its connections.
        httpClient.close()
        // Cancel the coroutine scope to clean up any lingering coroutines.
        scope.cancel()
    }
}
