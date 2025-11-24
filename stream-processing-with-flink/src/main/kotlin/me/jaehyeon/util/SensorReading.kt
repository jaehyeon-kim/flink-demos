package me.jaehyeon.util

import java.io.Serializable

/**
 * Represents a single sensor reading.
 *
 * This data class is the fundamental event type used throughout the Flink job.
 * It must be Serializable to be sent across the Flink cluster.
 *
 * @property id The unique identifier of the sensor.
 * @property timestamp The timestamp of the reading, in milliseconds since the epoch.
 * @property temperature The temperature value of the reading.
 */
data class SensorReading(
    val id: String,
    val timestamp: Long,
    val temperature: Double,
) : Serializable
