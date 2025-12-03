package me.jaehyeon.connector

import org.apache.flink.api.connector.source.SplitEnumerator
import org.apache.flink.api.connector.source.SplitEnumeratorContext

/**
 * The coordinator for the source. It runs on the JobManager.
 * Its job is to generate splits on demand from a configurable numeric range
 * and assign them to readers. It checkpoints its position in the cycle (`lastSeenId`).
 */
class HttpSplitEnumerator(
    private val context: SplitEnumeratorContext<HttpSplit>,
    private val baseUrlPattern: String,
    private val startId: Long,
    private val maxId: Long,
    restoredLastSeenId: Long?,
) : SplitEnumerator<HttpSplit, Long> {
    private var lastSeenId: Long = restoredLastSeenId ?: (startId - 1)

    override fun start() {}

    override fun handleSplitRequest(
        subtaskId: Int,
        requesterHostname: String?,
    ) {
        // 1. Calculate the next ID in the cycle.
        var nextId = lastSeenId + 1
        // 2. Apply the configurable wrap-around rule.
        if (nextId > maxId) {
            nextId = startId
        }

        // 3. Generate the URL and create the split.
        val url = baseUrlPattern.replace("{id}", nextId.toString())
        val split = HttpSplit(url)

        // 4. Assign the split to the requesting reader.
        context.assignSplit(split, subtaskId)

        // 5. CRITICAL: Update the state for the next request.
        this.lastSeenId = nextId
    }

    override fun addSplitsBack(
        splits: MutableList<HttpSplit>,
        subtaskId: Int,
    ) {
        // This source is cyclical and state-based, not queue-based. If a reader
        // fails, we don't need to re-add its specific splits. The cyclical logic
        // will naturally re-assign a split for that ID when its turn comes again.
    }

    override fun addReader(subtaskId: Int) {}

    // --- Checkpointing ---
    override fun snapshotState(checkpointId: Long): Long {
        // On checkpoint, save the last ID that was successfully assigned.
        return lastSeenId
    }

    override fun close() {}
}
