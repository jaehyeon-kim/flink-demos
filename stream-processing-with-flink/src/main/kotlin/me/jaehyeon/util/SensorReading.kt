package me.jaehyeon.util

/** Data class to hold the SensorReading data. */
data class SensorReading(
    val id: String,
    val timestamp: Long,
    val temperature: Double,
)
