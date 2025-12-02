# Window Operator

This document provides a comprehensive overview of Flink's Window API for DataStream processing, updated with detailed descriptions, Kotlin code examples, and insights from Flink's documentation. Windows are a fundamental concept for processing infinite data streams by splitting them into finite chunks, enabling aggregations and other computations.

## Table of Contents

- [Stream Types and Keying](#stream-types-and-keying)
- [Window Assigners and Time Semantics](#window-assigners-and-time-semantics)
- [Applying Window Functions](#applying-window-functions)
- [Understanding Triggers](#understanding-triggers)
- [Using Evictors](#using-evictors)
- [Window Lifecycle and State Management](#window-lifecycle-and-state-management)
- [Handling Late Data](#handling-late-data)

## Stream Types and Keying

The foundation of windowing lies in how the stream is partitioned. The choice between keyed and non-keyed windows has significant performance implications.

### Keyed Windows (Recommended)

For scalable and parallel processing, streams should be partitioned by a key. A `keyBy()` transformation logically divides the stream, and Flink maintains independent window states for each key. This allows computations to be distributed across multiple tasks.

**Signature:**

```kotlin
fun <T, K> DataStream<T>.keyBy(keySelector: KeySelector<T, K>): KeyedStream<T, K>
```

**Description:**
A `KeyedStream` is created by specifying a `keySelector` that extracts a key from each element. All subsequent window operations will be performed independently for each unique key.

**Code Snippet:**

```kotlin
// IN: DataStream<SensorReading>
val keyedStream: KeyedStream<SensorReading, String> = stream
    .keyBy { it.sensorId } // KeyedStream<SensorReading, String>

// Further operations are now keyed
val windowedStream = keyedStream.window(...)
```

### Non-Keyed Windows (Use with Caution)

Non-keyed windows, applied with `windowAll()`, are processed by a single task, creating a bottleneck with a parallelism of one. This approach should only be used for global aggregations where a single, unified result is absolutely necessary.

**Signature:**

```kotlin
fun <T> DataStream<T>.windowAll(assigner: WindowAssigner<in T, W>): AllWindowedStream<T, W>
```

**Description:**
An `AllWindowedStream` treats the entire stream as a single partition. Use this sparingly as it eliminates the possibility of parallel execution for the windowing logic.

**Code Snippet:**

```kotlin
// IN: DataStream<UserEvent>
// Calculates the total number of events across the entire system in 5-minute windows
val allWindowedStream: AllWindowedStream<UserEvent, TimeWindow> = stream
    .windowAll(TumblingProcessingTimeWindows.of(Duration.ofMinutes(5)))
```

## Window Assigners and Time Semantics

A `WindowAssigner` is responsible for assigning each incoming element to one or more `Window` objects. Flink provides several pre-built assigners for common use cases.

**Interface: `WindowAssigner`**

```kotlin
abstract class WindowAssigner<T, W : Window> {
    // Returns a collection of windows the element should belong to.
    abstract fun assignWindows(element: T, timestamp: Long, context: WindowAssignerContext): Collection<W>

    // Returns the default Trigger for this window type.
    abstract fun getDefaultTrigger(env: StreamExecutionEnvironment): Trigger<T, W>

    // Returns a serializer for the Window type.
    abstract fun getWindowSerializer(config: ExecutionConfig): TypeSerializer<W>

    // Informs the system if this assigner is based on event time.
    abstract fun isEventTime(): Boolean
}
```

### Standard Assigners

#### Time-Based Windows

These are the most common types of windows and are based on event time or processing time.

- **Tumbling Windows:** Fixed-size, non-overlapping windows. Each element belongs to exactly one window.

  ````kotlin
  // 5-minute tumbling windows based on event time
  .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))

  // Daily tumbling windows offset for a specific timezone (e.g., UTC-8)
  .window(TumblingEventTimeWindows.of(Duration.ofDays(1), Duration.ofHours(-8)))
  ```*   **Sliding Windows:** Fixed-size, overlapping windows. An element can belong to multiple windows. Defined by a size and a slide interval.
  ```kotlin
  // Window size: 10 minutes, Slide interval: 5 minutes
  // Each element will belong to two windows.
  .window(SlidingEventTimeWindows.of(Duration.ofMinutes(10), Duration.ofMinutes(5)))
  ````

#### Session Windows

Session windows group events by periods of activity, separated by a gap of inactivity. They are dynamic and do not have a fixed start or end time. The underlying mechanism involves assigning each element to a new window and subsequently merging overlapping windows.

```kotlin
// Creates a session window that closes after 10 minutes of inactivity.
.window(EventTimeSessionWindows.withGap(Duration.ofMinutes(10)))
```

#### Global Windows

This assigner places all elements with the same key into a single `GlobalWindow`. This is only useful when combined with a custom `Trigger`, as the default trigger for `GlobalWindow` never fires.

```kotlin
// Groups all elements for a key. Must be paired with a trigger to define processing logic.
.window(GlobalWindows.create())
.trigger(CountTrigger.of(100)) // Fires every 100 elements
```

## Applying Window Functions

Window functions define the computation to be performed on the elements collected in a window. There are two main approaches: incremental aggregation and full window processing.

### Incremental Aggregation (Efficient)

These functions process elements as they arrive, storing only a single aggregate value in the window's state. This is highly memory-efficient.

#### `ReduceFunction`

Combines the current aggregate with a new element. The input, output, and aggregate types must all be the same.

**Full Code Example:**

```kotlin
data class Transaction(val id: String, val amount: Double)

// Sum all transaction amounts for each ID in 1-minute windows.
val stream: DataStream<Transaction> = // ... from some source

val resultStream: DataStream<Transaction> = stream
    .keyBy { it.id }
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
    .reduce { t1, t2 -> Transaction(t1.id, t1.amount + t2.amount) }

resultStream.print()
```

#### `AggregateFunction`

The most versatile incremental aggregation interface. It allows the input, accumulator, and output types to be different, providing greater flexibility.

**Interface Signature:**

```kotlin
interface AggregateFunction<IN, ACC, OUT> : Function, Serializable {
    fun createAccumulator(): ACC
    fun add(value: IN, accumulator: ACC): ACC
    fun getResult(accumulator: ACC): OUT
    fun merge(a: ACC, b: ACC): ACC // Essential for merging windows, like in session windows.
}
```

**Full Code Example:**

```kotlin
data class SensorReading(val id: String, val value: Double)
data class AverageAccumulator(var sum: Double = 0.0, var count: Int = 0)

class AverageAggregator : AggregateFunction<SensorReading, AverageAccumulator, Double> {
    override fun createAccumulator() = AverageAccumulator()
    override fun add(value: SensorReading, acc: AverageAccumulator): AverageAccumulator {
        acc.sum += value.value
        acc.count++
        return acc
    }
    override fun getResult(acc: AverageAccumulator): Double = if (acc.count == 0) 0.0 else acc.sum / acc.count
    override fun merge(a: AverageAccumulator, b: AverageAccumulator): AverageAccumulator {
        a.sum += b.sum
        a.count += b.count
        return a
    }
}

// IN: DataStream<SensorReading>
val averageStream: DataStream<Double> = stream
    .keyBy { it.id }
    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(30)))
    .aggregate(AverageAggregator())

averageStream.print()
```

### Full Window Functions (Powerful but Resource-Intensive)

These functions buffer all elements of a window in state until the window is ready to be processed. This provides maximum flexibility but can consume significant memory.

#### `ProcessWindowFunction`

This function provides access to all elements in the window, plus contextual metadata about the window and time. It is the most powerful but least performant window function.

**Interface Signature (Simplified):**

```kotlin
abstract class ProcessWindowFunction<IN, OUT, KEY, W : Window> {
    abstract fun process(key: KEY, context: Context, elements: Iterable<IN>, out: Collector<OUT>)

    // Context provides access to time, state, and side outputs.
    abstract class Context {
        abstract val window: W
        abstract fun currentProcessingTime(): Long
        abstract fun currentWatermark(): Long
        abstract fun windowState(): KeyedStateStore
        abstract fun globalState(): KeyedStateStore
        abstract fun <X> output(outputTag: OutputTag<X>, value: X)
    }
}
```

### Combined Approach (Best Practice)

For efficiency and flexibility, combine an incremental aggregation function (`ReduceFunction` or `AggregateFunction`) with a `ProcessWindowFunction`. The incremental function pre-aggregates the results, and the `ProcessWindowFunction` receives only the single aggregated value, along with the window context.

**Full Code Example:**

```kotlin
data class WindowResult(val key: String, val endTime: Long, val average: Double)

class WindowResultProcess : ProcessWindowFunction<Double, WindowResult, String, TimeWindow>() {
    override fun process(key: String, context: Context, elements: Iterable<Double>, out: Collector<WindowResult>) {
        // 'elements' will contain only one value: the pre-aggregated average from AverageAggregator
        val average = elements.first()
        val windowEndTime = context.window().end
        out.collect(WindowResult(key, windowEndTime, average))
    }
}

// IN: DataStream<SensorReading> from the previous example
val detailedResultStream: DataStream<WindowResult> = stream
    .keyBy { it.id }
    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(30)))
    .aggregate(AverageAggregator(), WindowResultProcess())

detailedResultStream.print()
```

## Understanding Triggers

Triggers determine _when_ a window function is executed. Each `WindowAssigner` has a default trigger, but you can specify a custom one for more complex firing logic. Triggers can be stateful.

A `Trigger` responds to events by returning a `TriggerResult`:

- **CONTINUE**: Do nothing.
- **FIRE**: Evaluate the window function, but keep the window and its contents.
- **PURGE**: Discard the window's contents.
- **FIRE_AND_PURGE**: Evaluate the function, then discard the window and its contents.

**Interface Signature:**

```kotlin
abstract class Trigger<T, W : Window> {
    // Called for each element added to the window.
    abstract fun onElement(element: T, timestamp: Long, window: W, ctx: TriggerContext): TriggerResult
    // Called when a processing-time timer fires.
    abstract fun onProcessingTime(time: Long, window: W, ctx: TriggerContext): TriggerResult
    // Called when an event-time timer fires.
    abstract fun onEventTime(time: Long, window: W, ctx: TriggerContext): TriggerResult
    // Must clean up any per-window state.
    abstract fun clear(window: W, ctx: TriggerContext)
    // Optional: for merging state in stateful triggers on merging windows.
    open fun onMerge(window: W, ctx: OnMergeContext) {}
}
```

### Custom Trigger Example: Early and Final Results

This trigger fires when the element count reaches a threshold (early result) and also fires finally when the watermark passes the window end.

```kotlin
class EarlyResultTrigger(private val threshold: Int) : Trigger<Any, TimeWindow>() {
    private val countStateDesc = ValueStateDescriptor("count", Types.INT)

    override fun onElement(element: Any, timestamp: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult {
        ctx.registerEventTimeTimer(window.end)
        val countState = ctx.getPartitionedState(countStateDesc)
        val currentCount = countState.value() ?: 0
        val newCount = currentCount + 1
        countState.update(newCount)

        return if (newCount >= threshold) {
            countState.clear() // Reset count for next early firing
            TriggerResult.FIRE
        } else {
            TriggerResult.CONTINUE
        }
    }

    override fun onEventTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult {
        // Fire and purge only at the final watermark.
        return if (time == window.end) TriggerResult.FIRE_AND_PURGE else TriggerResult.CONTINUE
    }

    override fun onProcessingTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult {
        return TriggerResult.CONTINUE
    }

    override fun clear(window: TimeWindow, ctx: TriggerContext) {
        // Clear the state when the window is purged.
        ctx.getPartitionedState(countStateDesc).clear()
        ctx.deleteEventTimeTimer(window.end)
    }
}
```

## Using Evictors

Evictors are an optional component that can remove elements from a window _after_ a trigger fires but _before_ the window function is applied.

**Interface Signature:**

```kotlin
interface Evictor<T, W : Window> : Serializable {
    fun evictBefore(elements: Iterable<TimestampedValue<T>>, size: Int, window: W, evictorContext: EvictorContext)
    fun evictAfter(elements: Iterable<TimestampedValue<T>>, size: Int, window: W, evictorContext: EvictorContext)
}
```

**Important:** Using an evictor disables the memory benefits of incremental aggregation because Flink must buffer all window elements internally.

**Example: Keeping the last 3 elements**

```kotlin
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor

// ...
.window(...)
.trigger(...)
.evictor(CountEvictor.of(3, true)) // Keep 3 elements, evict before function
.apply(...)
```

## Window Lifecycle and State Management

1.  **Creation**: The first element assigned to a window creates it, and its state is initialized.
2.  **Processing**: Elements are added to the window. If an incremental function is used, the aggregate is updated. Otherwise, elements are buffered. Triggers may register timers.
3.  **Firing**: A `Trigger` returns `FIRE` or `FIRE_AND_PURGE`, causing the `WindowFunction` to execute.
4.  **Lateness**: If `allowedLateness` is set, the window and its state persist after the watermark passes its end. Late elements can trigger additional firings.
5.  **Purging**: The window and its contents are removed when the watermark passes `window.end + allowedLateness`. Flink cleans up the window's content state, but custom state from a `ProcessWindowFunction` or `Trigger` must be cleaned manually in the `clear()` method.

### State Management

- **Window State (`context.windowState()`)**: Scoped to a specific key _and_ a specific window instance. It is automatically garbage collected when the window is purged. Ideal for storing metadata about the current window.
- **Global State (`context.globalState()`)**: Scoped only to the key. It persists across all windows for that key and must be managed manually. Useful for calculations that compare the current window to historical data.

## Handling Late Data

Flink offers a three-level strategy for handling elements whose timestamps are older than the current watermark.

1.  **Default (Drop)**: By default, any element arriving after the watermark has passed its window's end is dropped.
2.  **`allowedLateness` (Update)**: You can keep a window's state alive for a specified duration after the watermark passes its end. Late elements arriving within this period will be processed and can trigger another firing, updating the window's result.
3.  **Side Output (Collect)**: Elements that arrive even after the allowed lateness period can be captured and sent to a separate stream for inspection or alternative processing.

**Full Code Example:**

```kotlin
val lateDataTag = OutputTag<SensorReading>("late-readings")

val resultStream = stream
    .keyBy { it.id }
    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
    // 1. Keep window state for an additional 5 seconds.
    .allowedLateness(Duration.ofSeconds(5))
    // 2. Send data arriving after that to a side output.
    .sideOutputLateData(lateDataTag)
    .aggregate(AverageAggregator())

// 3. Access the late data stream.
val lateStream: DataStream<SensorReading> = (resultStream as SingleOutputStreamOperator<*>)
    .getSideOutput(lateDataTag)

lateStream.printToErr("Late >>")
resultStream.print("Result >>")
```
