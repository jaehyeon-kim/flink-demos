package me.jaehyeon.util

import org.apache.flink.api.connector.source.SourceSplit
import java.io.Serializable

/**
 * Represents a split of work for the SensorSource.
 *
 * In the context of this custom source, a "split" is a logical unit of work assigned
 * to a single parallel SourceReader instance. Here, it simply wraps the subtask index
 * to ensure each parallel reader generates unique sensor IDs.
 * It must be Serializable to be sent from the SplitEnumerator (on the JobManager)
 * to the SourceReaders (on the TaskManagers).
 *
 * @property subtaskIndex The parallel instance index this split is for.
 */
data class SensorSplit(
    val subtaskIndex: Int,
) : SourceSplit,
    Serializable {
    /**
     * Provides a unique identifier for this split.
     */
    override fun splitId(): String = "split-$subtaskIndex"
}
