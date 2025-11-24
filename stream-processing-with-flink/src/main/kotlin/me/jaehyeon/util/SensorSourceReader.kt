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
 * in a dedicated background thread that continuously creates new `SensorReading`
 * objects and places them into a blocking queue (buffer).
 *
 * The `pollNext` method, called by the Flink runtime's event loop, retrieves records from this
 * queue. This decouples the data generation from Flink's main thread, ensuring
 * non-blocking behavior.
 *
 * @param readerContext The context provided by Flink, which gives access to runtime information.
 */
class SensorSourceReader(
    private val readerContext: SourceReaderContext,
) : SourceReader<SensorReading, SensorSplit> {
    // A thread-safe queue to buffer records between the generator thread and Flink's polling thread.
    private val buffer = LinkedBlockingQueue<SensorReading>(10)

    // Volatile flag to ensure visibility across threads for stopping the generator.
    @Volatile
    private var running = false

    // The dedicated thread that will run the data generation loop.
    private var generatorThread: Thread? = null

    override fun start() {
        // Mark the source as running. Data generation will begin once splits are assigned.
        running = true
    }

    override fun addSplits(splits: List<SensorSplit>) {
        // This source is designed to handle one split per reader.
        val split = splits.first()
        val taskIdx = split.subtaskIndex

        // Initialize and start the data generation thread.
        generatorThread =
            Thread {
                val rand = Random()
                // Initialize the state for 10 sensors with unique IDs and starting temperatures.
                var sensorData =
                    (1..10).map { i ->
                        "sensor_${taskIdx * 10 + i}" to (65 + (rand.nextGaussian() * 20))
                    }

                try {
                    // Loop until the source is canceled.
                    while (running) {
                        val curTime = System.currentTimeMillis()
                        // Update the temperature for each sensor based on the previous value.
                        sensorData =
                            sensorData.map { (id, temp) ->
                                val newTemp = temp + rand.nextGaussian() * 0.5
                                // Put the new reading into the buffer, blocking if it's full.
                                buffer.put(SensorReading(id, curTime, newTemp))
                                id to newTemp
                            }
                        // Wait for a short interval before generating the next readings.
                        Thread.sleep(100)
                    }
                } catch (e: InterruptedException) {
                    // Wait for a short interval before generating the next readings.
                }
            }
        generatorThread?.start()
    }

    override fun pollNext(output: ReaderOutput<SensorReading>): InputStatus {
        // Poll the buffer for a record without blocking.
        val reading = buffer.poll()

        return if (reading != null) {
            // If a record was available, collect it and signal that more might be available.
            output.collect(reading)
            InputStatus.MORE_AVAILABLE
        } else {
            // If the buffer is empty, signal that there is nothing available right now.
            InputStatus.NOTHING_AVAILABLE
        }
    }

    override fun isAvailable(): CompletableFuture<Void> {
        // Our source is always "available" because the background thread is always running.
        // Flink will poll `pollNext` immediately if this future is completed.
        return FutureUtils.completedVoidFuture()
    }

    override fun snapshotState(checkpointId: Long): MutableList<SensorSplit> {
        // This source is stateless and does not support checkpointing, so we return an empty list.
        return mutableListOf()
    }

    override fun notifyNoMoreSplits() {
        // This is a continuous, unbounded source, so this method is not relevant.
    }

    override fun close() {
        // Set the running flag to false in order to signal the generator thread to stop.
        running = false
        // Interrupt the thread to wake it up if it's sleeping.
        generatorThread?.interrupt()
        // Wait for the generator thread to finish its execution.
        generatorThread?.join()
    }
}
