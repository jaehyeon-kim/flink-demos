package me.jaehyeon.smoke

import org.apache.flink.api.connector.source.SourceSplit
import java.io.Serializable

/**
 * Represents a split for the SmokeLevelSource.
 *
 * Since each reader instance behaves identically, we only need a single split type.
 */
data class SmokeLevelSplit(
    private val id: String = "smoke-level-split",
) : SourceSplit,
    Serializable {
    override fun splitId(): String = id
}
