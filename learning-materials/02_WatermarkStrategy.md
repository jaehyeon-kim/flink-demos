# Watermark Strategies

This document provides a detailed overview of Watermark Strategies in the Apache Flink DataStream API. Watermarks are the fundamental mechanism in Flink that enables event-time processing, allowing the system to handle out-of-order data and reason about time-based operations like windowing. A `WatermarkStrategy` defines how these watermarks are generated for a data source.

## Table of Contents

- [Bounded Out-of-Orderness (Most Common)](#bounded-out-of-orderness-most-common)
- [Monotonously Increasing Timestamps (Strictly Ordered)](#monotonously-increasing-timestamps-strictly-ordered)
- [No Watermarks (Processing Time)](#no-watermarks-processing-time)
- [Custom Watermark Generator (Advanced)](#custom-watermark-generator-advanced)
- [Important Configuration: Handling Idle Sources](#important-configuration-handling-idle-sources)

---

## Bounded Out-of-Orderness (Most Common)

This is the standard and most widely used strategy, designed for real-world streams where events can arrive slightly out of order due to network latency, distributed systems, or source-specific behaviors.

- **What it is:** This strategy generates watermarks based on the highest timestamp observed in the stream so far, minus a specified maximum delay. The watermark's timestamp is calculated as `(max_seen_timestamp - max_delay)`. This signifies that the system does not expect any more elements with a timestamp older than this value.
- **When to use:** This should be your default choice for any out-of-order stream, such as those from Kafka, Kinesis, or Pulsar. It provides a robust balance between processing latency and correctness.
- **How to use:** You must provide the maximum expected out-of-orderness (lateness) as a `Duration`. A `TimestampAssigner` is also required to extract the event timestamp from each element.

**Interface Signature**

```kotlin
// Simplified for illustration
public static <T> WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness) {
    // ...
}

public interface TimestampAssigner<T> {
    long extractTimestamp(T element, long recordTimestamp);
}

// Chained method
public WatermarkStrategy<T> withTimestampAssigner(TimestampAssigner<T> timestampAssigner) {
    // ...
}
```

**Code Snippet**

```kotlin
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import java.time.Duration

WatermarkStrategy
    .forBoundedOutOfOrderness<MyEvent>(Duration.ofSeconds(5)) // Max lateness of 5 seconds
    .withTimestampAssigner { event, _ -> event.timestamp }
```

**Full Code Example**

```kotlin
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream
import java.time.Duration

// Data class representing an event with a timestamp
data class MyEvent(val key: String, val value: Int, val timestamp: Long)

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    // A stream of events that are slightly out of order
    val sourceStream: DataStream<MyEvent> = env.fromElements(
        MyEvent("A", 1, 1000L), // t=1s
        MyEvent("A", 2, 2000L), // t=2s
        MyEvent("A", 3, 4000L), // t=4s
        MyEvent("A", 4, 3000L), // Late event, t=3s
        MyEvent("A", 5, 5000L)  // t=5s
    )

    // Define the watermark strategy for a 2-second out-of-orderness bound
    val watermarkStrategy = WatermarkStrategy
        .forBoundedOutOfOrderness<MyEvent>(Duration.ofSeconds(2))
        .withTimestampAssigner { event, _ -> event.timestamp }

    val withWatermarks: DataStream<MyEvent> = sourceStream.assignTimestampsAndWatermarks(watermarkStrategy)

    withWatermarks
        .map { "Event: $it, Watermark: ${it.timestamp - 2000}" } // Illustrative watermark calculation
        .print()

    env.execute("Bounded Out-of-Orderness Example")
}
```

---

## Monotonously Increasing Timestamps (Strictly Ordered)

This is a specialized and highly efficient strategy suitable only for streams where events are guaranteed to arrive in perfect, ascending timestamp order.

- **What it is:** This strategy generates a watermark directly from the timestamp of the current event. The watermark's timestamp is `(current_event_timestamp - 1)`. This assumes no earlier events will ever arrive.
- **When to use:** Use this **only** when you can absolutely guarantee that events are perfectly ordered by their timestamps at the source. This is very rare in distributed systems. Using this on an out-of-order stream will result in incorrect results, as late data will be dropped.
- **How to use:** Similar to the bounded strategy, it requires a `TimestampAssigner` to extract the timestamp from each event.

**Interface Signature**

```kotlin
// Simplified for illustration
public static <T> WatermarkStrategy<T> forMonotonouslyIncreasingTimestamps() {
    // ...
}
```

**Code Snippet**

```kotlin
import org.apache.flink.api.common.eventtime.WatermarkStrategy

WatermarkStrategy
    .forMonotonouslyIncreasingTimestamps<MyEvent>()
    .withTimestampAssigner { event, _ -> event.timestamp }
```

**Full Code Example**

```kotlin
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream

data class OrderedEvent(val id: Int, val timestamp: Long)

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    // A stream where events are perfectly ordered by timestamp
    val orderedSource: DataStream<OrderedEvent> = env.fromElements(
        OrderedEvent(1, 1000L),
        OrderedEvent(2, 2000L),
        OrderedEvent(3, 3000L),
        OrderedEvent(4, 4000L)
    )

    // Define the watermark strategy for strictly increasing timestamps
    val watermarkStrategy = WatermarkStrategy
        .forMonotonouslyIncreasingTimestamps<OrderedEvent>()
        .withTimestampAssigner { event, _ -> event.timestamp }

    val withWatermarks: DataStream<OrderedEvent> = orderedSource.assignTimestampsAndWatermarks(watermarkStrategy)

    withWatermarks
        .map { "Event: $it, Watermark: ${it.timestamp - 1}" } // Illustrative watermark calculation
        .print()

    env.execute("Monotonously Increasing Timestamps Example")
}
```

---

## No Watermarks (Processing Time)

This strategy explicitly disables event-time semantics and watermark generation for a source.

- **What it is:** This strategy does not generate any watermarks. As a result, time-based operations like windows will operate in **processing time**, meaning they will be triggered based on the system clock of the machine executing the task, not the timestamps within the events.
- **When to use:** Use this when your data source has no timestamps, or when you explicitly intend to use processing time for all time-based operations.
- **How to use:** Simply assign this strategy to your source stream.

**Interface Signature**

```kotlin
// Simplified for illustration
public static <T> WatermarkStrategy<T> noWatermarks() {
    // ...
}
```

**Code Snippet**

```kotlin
import org.apache.flink.api.common.eventtime.WatermarkStrategy

WatermarkStrategy.noWatermarks<MyEvent>()
```

**Full Code Example**

```kotlin
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

data class EventWithoutTimestamp(val key: String, val value: Int)

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val sourceStream: DataStream<EventWithoutTimestamp> = env.fromElements(
        EventWithoutTimestamp("A", 1),
        EventWithoutTimestamp("A", 2),
        EventWithoutTimestamp("B", 3)
    )

    // Assign the noWatermarks strategy. This is often the default if none is specified,
    // but it is good practice to be explicit.
    val withNoWatermarks = sourceStream.assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks())

    // Perform a window operation based on processing time
    withNoWatermarks
        .keyBy { it.key }
        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
        .sum("value")
        .print()


    env.execute("No Watermarks (Processing Time) Example")
}
```

---

## Custom Watermark Generator (Advanced)

For full control over watermark emission logic, you can implement your own `WatermarkGenerator`.

- **What it is:** A custom class where you define the exact logic for generating watermarks. It provides two key methods:
  - `onEvent()`: Called for every event. This allows you to inspect event data and decide whether to emit a watermark. This is known as the **punctuated** style, where watermarks can be emitted on a per-event basis.
  - `onPeriodicEmit()`: Called periodically by Flink (the interval is configured in `ExecutionConfig`). This is used to emit watermarks based on information gathered from events since the last call. This is the **periodic** style, which the built-in `forBoundedOutOfOrderness` strategy uses.
- **When to use:** When you have complex requirements not covered by the built-in strategies. For example, generating watermarks only after seeing a special "end-of-batch" event, or implementing different watermarking logic per Kafka partition.

**Interface Signatures**

```kotlin
// Simplified for illustration
public interface WatermarkGenerator<T> {
    void onEvent(T event, long eventTimestamp, WatermarkOutput output);
    void onPeriodicEmit(WatermarkOutput output);
}

public interface WatermarkStrategy<T> extends TimestampAssignerSupplier<T>, WatermarkGeneratorSupplier<T> {}
```

**Full Code Example**
This example demonstrates a periodic watermark generator that assumes timestamps within each key (partition) are roughly in order.

```kotlin
import org.apache.flink.api.common.eventtime.*
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream
import java.time.Duration

data class MyEvent(val key: String, val value: Int, val timestamp: Long)

// Custom generator that tracks max timestamp per key
class PerKeyWatermarkGenerator(val maxOutOfOrderness: Duration) : WatermarkGenerator<MyEvent> {
    private val maxTimestamps = mutableMapOf<String, Long>()
    private var overallMaxTimestamp = Long.MIN_VALUE

    override fun onEvent(event: MyEvent, eventTimestamp: Long, output: WatermarkOutput) {
        val currentMax = maxTimestamps.getOrDefault(event.key, Long.MIN_VALUE)
        maxTimestamps[event.key] = maxOf(currentMax, eventTimestamp)
        overallMaxTimestamp = maxOf(overallMaxTimestamp, eventTimestamp)
    }

    override fun onPeriodicEmit(output: WatermarkOutput) {
        val watermarkTime = overallMaxTimestamp - maxOutOfOrderness.toMillis()
        output.emitWatermark(Watermark(watermarkTime))
    }
}

// Strategy to supply the custom generator
class PerKeyWatermarkStrategy(val delay: Duration) : WatermarkStrategy<MyEvent> {
    override fun createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner<MyEvent> {
        return TimestampAssigner { event, _ -> event.timestamp }
    }

    override fun createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator<MyEvent> {
        return PerKeyWatermarkGenerator(delay)
    }
}

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val sourceStream: DataStream<MyEvent> = env.fromElements(
        MyEvent("A", 1, 1000L), MyEvent("B", 1, 1100L),
        MyEvent("A", 2, 2000L), MyEvent("B", 2, 2200L)
    )

    val withCustomWatermarks = sourceStream.assignTimestampsAndWatermarks(PerKeyWatermarkStrategy(Duration.ofSeconds(1)))

    withCustomWatermarks.print()

    env.execute("Custom Watermark Generator Example")
}
```

---

## Important Configuration: Handling Idle Sources

A common production issue arises when one parallel instance of a source (e.g., a consumer reading from an idle Kafka partition) stops sending data. Its local watermark will not advance, which can stall the entire application's event time clock because the global watermark is the minimum of all parallel source watermarks.

- **`withIdleness()`**: This configuration on the `WatermarkStrategy` addresses the idle source problem. It marks a source subtask as idle if it hasn't produced an event for a configured duration. Once idle, its watermark is ignored by downstream operators when they calculate their combined watermark, allowing the application's event time to continue advancing based on the active sources.

- **How to use:** Chain the `.withIdleness()` call on your `WatermarkStrategy` definition, providing a timeout duration.

**Code Snippet**

```kotlin
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import java.time.Duration

WatermarkStrategy
    .forBoundedOutOfOrderness<MyEvent>(Duration.ofSeconds(5))
    .withIdleness(Duration.ofMinutes(1)) // Mark as idle after 1 minute of no events
    .withTimestampAssigner { event, _ -> event.timestamp }
```
