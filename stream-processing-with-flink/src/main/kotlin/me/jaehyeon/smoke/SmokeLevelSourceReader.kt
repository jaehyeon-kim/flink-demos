package me.jaehyeon.smoke

import org.apache.flink.api.connector.source.ReaderOutput
import org.apache.flink.api.connector.source.SourceReader
import org.apache.flink.api.connector.source.SourceReaderContext
import org.apache.flink.core.io.InputStatus
import org.apache.flink.util.concurrent.FutureUtils
import java.util.Random
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingQueue

/**
 * The SourceReader for the SmokeLevelSource. It runs on TaskManagers and
 * generates the stream of SmokeLevel events in a background thread.
 */
class SmokeLevelSourceReader(
    private val readerContext: SourceReaderContext,
) : SourceReader<SmokeLevel, SmokeLevelSplit> {
    private val buffer = LinkedBlockingQueue<SmokeLevel>(1)

    @Volatile
    private var running = false
    private var generatorThread: Thread? = null

    override fun start() {
        running = true
    }

    override fun addSplits(splits: List<SmokeLevelSplit>) {
        generatorThread =
            Thread {
                val rand = Random()
                try {
                    while (running) {
                        val smokeLevel = if (rand.nextGaussian() > 0.8) SmokeLevel.High else SmokeLevel.Low
                        buffer.put(smokeLevel)
                        Thread.sleep(1000)
                    }
                } catch (e: InterruptedException) {
                    // Thread interrupted, exit
                }
            }
        generatorThread?.start()
    }

    override fun pollNext(output: ReaderOutput<SmokeLevel>): InputStatus? {
        val level = buffer.poll()
        return if (level != null) {
            output.collect(level)
            InputStatus.MORE_AVAILABLE
        } else {
            InputStatus.NOTHING_AVAILABLE
        }
    }

    override fun isAvailable(): CompletableFuture<Void> = FutureUtils.completedVoidFuture()

    override fun snapshotState(checkpointId: Long): List<SmokeLevelSplit> = mutableListOf()

    override fun notifyNoMoreSplits() {}

    override fun close() {
        running == false
        generatorThread?.interrupt()
        generatorThread?.join()
    }
}
