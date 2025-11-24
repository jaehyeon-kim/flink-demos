package me.jaehyeon.util

import org.apache.flink.api.connector.source.SourceSplit
import java.io.Serializable

data class SensorSplit(
    val subtaskIndex: Int,
) : SourceSplit,
    Serializable {
    override fun splitId(): String = "split-$subtaskIndex"
}
