package me.jaehyeon.util

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import java.time.Duration

/**
 * Assigns timestamps to SensorReadings based on their internal timestamp and
 * emits watermarks with five seconds of slack.
 */
class SensorTimeAssigner : BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Duration.ofSeconds(5)) {
    override fun extractTimestamp(element: SensorReading): Long = element.timestamp
}
