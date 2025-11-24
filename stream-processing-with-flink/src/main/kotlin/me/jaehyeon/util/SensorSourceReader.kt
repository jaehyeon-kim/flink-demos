package me.jaehyeon.util

import org.apache.flink.api.connector.source.ReaderOutput
import org.apache.flink.api.connector.source.SourceReader
import org.apache.flink.api.connector.source.SourceReaderContext
import org.apache.flink.core.io.InputStatus
import org.apache.flink.util.concurrent.FutureUtils
import java.util.Random
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingQueue

/**
 * A Flink `SourceReader` for the `SensorSource`. This class is instantiated on the
 * TaskManagers and is responsible for the actual data generation.
 *
 * It operates by receiving a single `SensorSplit`, which informs it of the unique
 * subtask index to use for generating sensor IDs. Data generation is performed
- * in a dedicated background thread that continuously creates new `SensorReading`
 * objects and places them into a blocking queue.
 *
 * The `pollNext` method, called by the Flink runtime, retrieves records from this
 * queue. This decouples the data generation from Flink's event loop, ensuring
 * non-blocking behavior.
 *
 * @param readerContext The context provided by Flink, which gives access to runtime information.
 */
class SensorSourceReader(
    private val readerContext: SourceReaderContext,
) : SourceReader<SensorReading, SensorSplit> {
    private val buffer = LinkedBlockingQueue<SensorReading>(10)

    @Volatile
    private var running = false
    private var generatorThread: Thread? = null

    override fun start() {
        running = true
    }

    override fun addSplits(splits: List<SensorSplit>) {
        val split = splits.first()
        val taskIdx = split.subtaskIndex

        generatorThread =
            Thread {
                val rand = Random()
                var sensorData =
                    (1..10).map { i ->
                        "sensor_${taskIdx * 10 + i}" to (65 + (rand.nextGaussian() * 20))
                    }

                try {
                    while (running) {
                        val curTime = System.currentTimeMillis()
                        sensorData =
                            sensorData.map { (id, temp) ->
                                val newTemp = temp + rand.nextGaussian() * 0.5
                                buffer.put(SensorReading(id, curTime, newTemp))
                                id to newTemp
                            }
                        Thread.sleep(100)
                    }
                } catch (e: InterruptedException) {
                    // Thread interrupted, exit.
                }
            }
        generatorThread?.start()
    }

    override fun pollNext(output: ReaderOutput<SensorReading>): InputStatus {
        val reading = buffer.poll()

        return if (reading != null) {
            output.collect(reading)
            InputStatus.MORE_AVAILABLE
        } else {
            InputStatus.NOTHING_AVAILABLE
        }
    }

    /**
     * Called by Flink to see if data is ready. We return a completed future
     * because our source is push-based (data is always being generated in the background).
     */
    override fun isAvailable(): CompletableFuture<Void> = FutureUtils.completedVoidFuture()

    /**
     * This source is stateless, so we return an empty list.
     * The signature now correctly returns MutableList<SensorSplit>.
     */
    override fun snapshotState(checkpointId: Long): MutableList<SensorSplit> = mutableListOf()

    /**
     * Called when the enumerator signals there are no more splits.
     * For a continuous source, this is not typically used.
     */
    override fun notifyNoMoreSplits() {
        // No action needed for this unbounded source.
    }

    override fun close() {
        running = false
        generatorThread?.interrupt()
        generatorThread?.join()
    }
}
