package me.jaehyeon.smoke

import java.io.Serializable

/**
 * Represents a smoke level alert.
 * A `data class` is the idiomatic Kotlin way to create a class
 * whose primary purpose is to hold data. The compiler automatically
 * generates `equals()`, `hashCode()`, `toString()`, and `copy()` methods,
 * which is crucial for Flink operations.
 */
data class Alert(
    val sensorId: String,
    val timestamp: Long,
    val message: String,
) : Serializable
