# DateStream API

## Transformations

### Basic Transformations (Element-wise)

These are typically stateless operations that are applied to each element in a stream independently.

- **`map`**: A 1-to-1 transformation. It takes one element and produces exactly one transformed element.
- **`filter`**: A 1-to-0/1 transformation. For each element, it evaluates a boolean condition and either keeps the element or discards it.
- **`flatMap`**: A 1-to-N transformation. For each input element, it can produce zero, one, or many output elements by using a `Collector`.

### KeyedStream Transformations

These transformations are performed on a `KeyedStream`, which is a stream that has been partitioned by a key. All operations are performed independently for each key.

- **`keyBy`**: The operator that converts a `DataStream` into a `KeyedStream`. It repartitions the stream so that all elements with the same key are sent to the same physical task instance.
- **`reduce`**: A rolling aggregation operator. It maintains a state for each key and combines the current state with each new element to produce a new state, which is then emitted downstream. It is used for implementing patterns like "rolling sums" or "running maximums".

### Multi-Stream Transformations

These operators combine or split multiple logical streams.

- **`union`**: Merges two or more streams of the **same data type** into a single stream. The elements are simply interleaved.
- **`connect`**: Merges two streams of **potentially different data types**. The streams remain logically separate, but they can be processed by a single `CoProcessFunction` (or similar), which is ideal for applying logic based on shared state.
- **Side Outputs**: Splits a **single stream** into multiple streams based on some logic. Inside a `ProcessFunction`, you can emit data to a main output and multiple secondary "side" outputs, which can be retrieved downstream as separate `DataStream`s.

### Distribution (Partitioning) Transformations

These transformations control how data is physically sent from one parallel operator instance to another. They are used to manage parallelism and control data shuffling across the network.

- **`shuffle` (Random)**: Distributes elements randomly and evenly to the downstream operator instances.
- **`rebalance` (Round-Robin)**: Distributes elements in a round-robin fashion to the downstream instances, ensuring an even workload. This is useful for mitigating data skew.
- **`rescale`**: A more efficient, localized round-robin. It only distributes elements to a subset of downstream instances, minimizing network traffic. Use this when you know the upstream and downstream operators have the same parallelism and a local connection is possible.
- **`broadcast`**: Sends **every** element to **every** downstream operator instance. This is used for "control" streams where all tasks need the same information (e.g., rules, patterns, configuration).
- **`global`**: Sends **all** elements to a **single** downstream operator instance (specifically, task instance 0). This creates a bottleneck and should be used with caution.
- **`partitionCustom`**: Allows you to implement a custom partitioning logic. You provide a `Partitioner` function that determines exactly which downstream instance an element should be sent to based on a key. `keyBy` is a specific, highly optimized implementation of this pattern.

## Watermark Strategy

A `WatermarkStrategy` tells Flink how to generate watermarks, which are the mechanism that drives event time forward in a streaming job.

### Bounded Out-of-Orderness (Most Common)

This is the standard strategy for nearly all real-world streams where events can arrive slightly out of order.

- **What it is:** Generates watermarks based on the highest timestamp seen so far, minus a specified maximum delay. The watermark is `(max_timestamp - max_delay)`.
- **When to use:** For any out-of-order stream from sources like Kafka or Kinesis. This should be your default choice.
- **How to use:**
  ```kotlin
  WatermarkStrategy
      .forBoundedOutOfOrderness<MyEvent>(Duration.ofSeconds(5)) // Max lateness of 5 seconds
      .withTimestampAssigner { event, _ -> event.timestamp }
  ```

### Monotonously Increasing Timestamps (Strictly Ordered)

This is a specialized and highly efficient strategy for perfectly ordered streams.

- **What it is:** Generates a watermark directly from the timestamp of the current event. The watermark is `(timestamp - 1)`.
- **When to use:** Only when you can **absolutely guarantee** that events will arrive in perfect ascending timestamp order. This is rare in practice.
- **How to use:**
  ```kotlin
  WatermarkStrategy
      .forMonotonouslyIncreasingTimestamps<MyEvent>()
      .withTimestampAssigner { event, _ -> event.timestamp }
  ```

### No Watermarks (Processing Time)

This strategy explicitly disables event time and watermark generation for a source.

- **What it is:** Does not generate any watermarks.
- **When to use:** When your source has no timestamps and you intend to use **processing time** for all time-based operations (like windowing).
- **How to use:**
  ```kotlin
  WatermarkStrategy.noWatermarks()
  ```

### Custom Watermark Generator (Advanced)

For full control, you can implement your own `WatermarkGenerator`.

- **What it is:** A custom class where you define the exact logic for emitting watermarks. It has two key methods:
  - `onEvent()`: Called for every event. Use this to inspect events and emit watermarks on-the-fly (**punctuated** style).
  - `onPeriodicEmit()`: Called periodically by Flink. Use this to emit watermarks based on the highest timestamp seen since the last call (**periodic** style, like `forBoundedOutOfOrderness`).
- **When to use:** When you have complex requirements that the built-in strategies don't cover, such as different watermarking logic per partition or based on special events in the stream.

### Important Configuration: Handling Idle Sources

A common problem is when one parallel source instance (e.g., a Kafka partition) becomes idle and stops sending data. Its watermark will not advance, which can stall your entire application.

- **`withIdleness()`**: This configuration marks a source as idle if it hasn't produced an event for a configured duration. An idle source's watermark is then ignored by downstream operators, allowing the application's event time to continue advancing.

- **How to use:**
  ```kotlin
  WatermarkStrategy
      .forBoundedOutOfOrderness<MyEvent>(Duration.ofSeconds(5))
      .withIdleness(Duration.ofMinutes(1)) // Mark as idle after 1 minute of no events
      .withTimestampAssigner { event, _ -> event.timestamp }
  ```

## Process Function

A `ProcessFunction` is Flink's most powerful low-level operator. It provides direct access to the fundamental building blocks of any stateful streaming application: **state**, **timers**, and **side outputs**. You use it when standard operators (`map`, `filter`, windowing, etc.) are not expressive enough for your custom logic. It processes elements one by one.

### Key Features and Components

#### State Management

A `ProcessFunction` can be stateful. When applied to a `KeyedStream`, the state is automatically partitioned by key, meaning each key has its own independent state.

- **How it works:** You declare state objects (e.g., `ValueState`, `MapState`) in the `open()` method. Flink manages checkpointing and recovery.
- **Use Case:** Storing information needed to correlate events over time, such as the last seen value, a running count, or a complex state machine.

#### Emitting to Side Outputs

A `ProcessFunction` can emit data to multiple streams, not just the main one. This is the modern, type-safe way to route or split a stream.

- **How it works:**
  1.  Define a static `OutputTag<T>` to identify the side stream with a name and type.
  2.  Inside `processElement` or `onTimer`, use `ctx.output(myTag, data)` to send data to that specific side stream.
  3.  The main output is still sent via `out.collect(data)`.
- **Use Case:** Routing data. A common pattern is to send valid, processed data to the main output while routing malformed events, late data, or exceptions to a side output for separate logging or reprocessing.

#### `TimerService` and Timers

This is a core feature that allows you to register callbacks ("timers") to be executed at a specific time in the future. Timers are always scoped to the current key and are only available on **keyed streams**. You access the `TimerService` via the `Context` object (`ctx.timerService()`).

The `TimerService` API includes:

- **`currentProcessingTime(): Long`**: Returns the current wall-clock time of the machine executing the operator.
- **`currentWatermark(): Long`**: Returns the timestamp of the current watermark. This represents the current event-time progress and is the primary way to reason about "lateness".
- **`registerProcessingTimeTimer(timestamp: Long)`**: Registers a timer for the current key. The `onTimer()` method will be called when the machine's processing time reaches the provided `timestamp`.
- **`registerEventTimeTimer(timestamp: Long)`**: Registers a timer for the current key. The `onTimer()` method will be called when the stream's watermark is updated to a timestamp equal to or larger than the timer's `timestamp`.
- **`deleteProcessingTimeTimer(timestamp: Long)`**: Deletes a previously registered processing-time timer.
- **`deleteEventTimeTimer(timestamp: Long)`**: Deletes a previously registered event-time timer.

When a timer fires, the special callback method `onTimer()` is invoked, where you implement the time-based logic.

### Process Function Family

Flink provides several variants of the `ProcessFunction` to match different stream types.

- **`KeyedProcessFunction` (Most Common)**: Used on a `KeyedStream`. Has access to **keyed state** and the full **`TimerService`**. This is the workhorse for most complex event-driven logic.

- **`CoProcessFunction` (Non-Keyed, Connected)**: This function is used to process a non-keyed `ConnectedStreams` (from `streamA.connect(streamB)`).

  - **Key Features:** It has two distinct processing methods: `processElement1()` and `processElement2()`, one for each of the two input streams.
  - **State and Timers:** It can use operator state but **no keyed state** and **no timers**.
  - **Use Case:** Applying logic to two non-keyed streams, often involving a broadcast stream where one stream updates operator state that the other reads.

- **`KeyedCoProcessFunction` (Keyed, Connected)**: The keyed version of `CoProcessFunction`. It processes a `ConnectedStreams` where at least one stream is keyed.

  - **Key Features:** It has two `processElement` methods that share access to the same **keyed state** and **`TimerService`**.
  - **Use Case:** Ideal for implementing complex interactions between two keyed streams, such as enriching a user activity stream with updates from a user profile stream.

- **`ProcessFunction` (Non-Keyed)**: The simplest variant, used on a non-keyed `DataStream`. It can use operator state and side outputs but has **no keyed state** and **no timers**.

- **`ProcessWindowFunction`**: Used on a `WindowedStream`. Its `process` method is called once per window and receives an `Iterable` of all elements. It has access to per-window state but **no `TimerService`**.

- **`BroadcastProcessFunction` & `KeyedBroadcastProcessFunction`**: Specialized functions for the Broadcast State Pattern. They process a regular stream and a broadcast stream, allowing the broadcast stream to update a special "broadcast state" that is replicated to all parallel instances.

## States

Flink provides a rich set of state primitives that are checkpointed and managed by the runtime. They are broadly categorized into **Keyed State** and **Operator State**.

### 1. Keyed State

This is the most common type of state. It is scoped to a specific key within a `KeyedStream` and can only be used in functions applied to a `KeyedStream` (like a `KeyedProcessFunction`).

---

#### **`ValueState<T>`**

- **Data Structure:** Holds a single, updatable value of type `T`.
- **Use Case:** The workhorse for most stateful logic. Perfect for storing the "last seen" event, a running count, a state machine's current state, or the timestamp of a registered timer.
- **Key Methods:** `value()` to get, `update(T)` to set/overwrite.

---

#### **`ListState<T>`**

- **Data Structure:** Holds a `List` of elements of type `T`.
- **Use Case:** Buffering or collecting a list of events for a key that need to be processed together later (e.g., when a timer fires).
- **Key Methods:** `add(T)` to add one element, `addAll(List<T>)` to add multiple, `get()` to retrieve an `Iterable<T>`, and `clear()`.

---

#### **`MapState<K, V>`**

- **Data Structure:** Holds a `Map` of key-value pairs.
- **Use Case:** The most flexible state primitive. Ideal for complex aggregations or when you need to manage multiple attributes per key. For example, counting occurrences of different sub-categories for a given user key.
- **Key Methods:** `get(K)`, `put(K, V)`, `contains(K)`, `remove(K)`, `entries()`, `keys()`, `values()`.

---

#### **`ReducingState<T>`**

- **Data Structure:** Holds a single value of type `T`, similar to `ValueState`, but with a built-in aggregation mechanism.
- **How it Works:** You provide a `ReduceFunction` when you create it. Every time you add a new element, the state automatically combines the new element with the current value using your function.
- **Use Case:** Efficiently implementing simple, continuous aggregations like a rolling sum, min, or max, without the manual `get-update-write` pattern of `ValueState`.
- **Key Methods:** `add(T)` to add and reduce, `get()` to retrieve the current aggregated value.

---

#### **`AggregatingState<IN, OUT>`**

- **Data Structure:** The most general aggregation state. Holds a single value that is the result of an aggregation.
- **How it Works:** You provide a full `AggregateFunction`, which can have different input, accumulator, and output types.
- **Use Case:** For complex aggregations where the intermediate state (accumulator) is different from the input or output types. The canonical example is calculating an average, where the accumulator is a `(sum, count)` pair.
- **Key Methods:** `add(IN)` to add an element to the aggregation, `get()` to retrieve the final result.

### 2. Operator State

This state is scoped to a parallel operator instance, not a key. It is most commonly used in sources and sinks.

- **`ListState<T>`**: The most common type. The state is a list of elements. When parallelism changes, Flink can redistribute the state from the old instances' lists to the new instances. A classic example is a Kafka source, where each parallel instance uses `ListState` to store the topic partition offsets it is responsible for.
- **`BroadcastState<K, V>`**: A special type of operator state used in the Broadcast State Pattern. It is a `Map` whose contents are identical across all parallel instances of an operator. It is used to broadcast a low-volume stream of control data (e.g., rules) to all tasks.

This is a comprehensive, deep-dive reference guide to **Flink Window Operators**. It details the APIs, internal mechanisms, interfaces, and provides Kotlin code snippets for implementing custom logic.

---

## Window Operators

Windows bucket infinite streams into finite chunks for computation.

### 1. Stream Types & Keying

The foundation of windowing is whether the stream is partitioned.

#### Keyed Windows (Recommended)

Calculations are parallelized. Each logical key has its own independent window state.

```kotlin
// IN: DataStream<MyEvent>
stream
    .keyBy { it.userId }       // KeyedStream<MyEvent, String>
    .window(...)               // WindowedStream<MyEvent, String, Window>
```

#### Non-Keyed Windows (Bottleneck)

All data flows to a single task (parallelism = 1). Use only for global aggregations (e.g., "Total users in system").

```kotlin
stream.windowAll(...)          // AllWindowedStream<MyEvent, Window>
```

---

### 2. Time Semantics & Assigners

The `WindowAssigner` assigns elements to one or more `Window` objects.

#### Interface: `WindowAssigner`

```kotlin
abstract class WindowAssigner<T, W : Window> {
    // Returns a collection of windows the element belongs to
    abstract fun assignWindows(element: T, timestamp: Long, context: WindowAssignerContext): Collection<W>

    // Returns the default Trigger for this window type
    abstract fun getDefaultTrigger(env: StreamExecutionEnvironment): Trigger<T, W>

    // Used for type serialization
    abstract fun getWindowSerializer(config: ExecutionConfig): TypeSerializer<W>

    // Is this event time? (Crucial for Flink's internal logic)
    abstract fun isEventTime(): Boolean
}
```

#### Standard Assigners

##### A. Time-Based Windows

- **Tumbling Windows:** `[0, 5), [5, 10), ...`
  ```kotlin
  .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
  // With Offset (e.g., UTC+8 daily windows)
  .window(TumblingEventTimeWindows.of(Duration.ofDays(1), Duration.ofHours(-8)))
  ```
- **Sliding Windows:** `[0, 10), [5, 15), [10, 20)...`
  ```kotlin
  // Window size: 10m, Slide: 5m (elements belong to 2 windows)
  .window(SlidingEventTimeWindows.of(Duration.ofMinutes(10), Duration.ofMinutes(5)))
  ```

##### B. Session Windows (Merging)

Session windows group events by activity. They require a `MergingWindowAssigner`.

- **Logic:** Assigns every element to a new window `[ts, ts + gap]`. Then, the framework merges overlapping windows.
  ```kotlin
  .window(EventTimeSessionWindows.withGap(Duration.ofMinutes(10)))
  ```

##### C. Global Windows

Assigns everything to a single window. **Requires a custom Trigger** (default trigger never fires).

```kotlin
.window(GlobalWindows.create())
.trigger(CountTrigger.of(1000)) // Must define when to fire
```

---

### 3. Window Functions

How data in the window is processed.

#### A. Incremental Aggregation (Efficient)

Computes a result _as elements arrive_. Only the accumulator is stored in state.

##### `ReduceFunction`

Combines two inputs into one. Input and Output types must be the same.

```kotlin
class SumReduce : ReduceFunction<Long> {
    override fun reduce(value1: Long, value2: Long): Long = value1 + value2
}
```

##### `AggregateFunction`

The most flexible interface. Supports different types for Input, Accumulator, and Output.

```kotlin
interface AggregateFunction<IN, ACC, OUT> {
    fun createAccumulator(): ACC
    fun add(value: IN, accumulator: ACC): ACC
    fun getResult(accumulator: ACC): OUT
    fun merge(a: ACC, b: ACC): ACC // Required for Session Windows
}
```

#### B. Full Window Functions (Powerful)

Buffers _all_ elements (unless combined with incremental) until the trigger fires.

##### `ProcessWindowFunction`

Provides access to `Context` and window metadata.

```kotlin
abstract class ProcessWindowFunction<IN, OUT, KEY, W : Window> {
    /**
     * @param key The key for this window.
     * @param context Context for metadata (watermark, state, side outputs).
     * @param elements Iterable of all elements in the window.
     * @param out Collector for results.
     */
    abstract fun process(key: KEY, context: Context, elements: Iterable<IN>, out: Collector<OUT>)

    // Lifecycle methods
    fun open(parameters: Configuration) {}
    fun close() {}
    fun clear(context: Context) {} // Called when window is purged

    // Inner Context Interface
    abstract class Context {
        abstract fun currentProcessingTime(): Long
        abstract fun currentWatermark(): Long
        abstract fun windowState(): KeyedStateStore // Per-window state
        abstract fun globalState(): KeyedStateStore // Per-key global state
        abstract fun <X> output(outputTag: OutputTag<X>, value: X) // Side outputs
    }
}
```

#### C. Combined Approach (Best Practice)

Passes the result of the incremental aggregation to the full window function.

```kotlin
input
    .keyBy(...)
    .window(...)
    .aggregate(
        AverageAggregator(),    // Pre-aggregates to (Count, Sum)
        WindowResultProcess()   // Receives (Count, Sum), adds Window End Time
    )
```

---

### 4. Triggers

Triggers determine **when** a window is evaluated or purged. They react to signals (element arrival, timers).

#### `TriggerResult` Enum

- **CONTINUE**: Do nothing.
- **FIRE**: Invoke WindowFunction. Retain window contents (for multiple firings).
- **PURGE**: Clear window contents. Do _not_ evaluate function.
- **FIRE_AND_PURGE**: Invoke function, then clear contents (Standard closing behavior).

#### `Trigger` Interface

```kotlin
abstract class Trigger<T, W : Window> {
    // Called for every element added to the window
    abstract fun onElement(element: T, timestamp: Long, window: W, ctx: TriggerContext): TriggerResult

    // Called when a registered processing-time timer fires
    abstract fun onProcessingTime(time: Long, window: W, ctx: TriggerContext): TriggerResult

    // Called when the watermark passes a registered event-time timer
    abstract fun onEventTime(time: Long, window: W, ctx: TriggerContext): TriggerResult

    // Called when the window is purged. CRITICAL for clearing state!
    abstract fun clear(window: W, ctx: TriggerContext)

    // (Optional) Called when merging windows (Session Windows)
    fun onMerge(window: W, ctx: OnMergeContext) {}
}
```

#### Custom Trigger Example: Early Results

This simplified trigger fires if the watermark passes the window end (standard) OR if the element count hits a threshold (early result).

```kotlin
class EarlyResultTrigger(val threshold: Int) : Trigger<Any, TimeWindow>() {
    // Note: State handling (ValueState for count) omitted for brevity

    override fun onElement(element: Any, ts: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult {
        // ... increment count state ...
        if (count >= threshold) {
             return TriggerResult.FIRE // Fire early, don't purge
        }
        ctx.registerEventTimeTimer(window.end) // Ensure standard firing
        return TriggerResult.CONTINUE
    }

    override fun onEventTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult {
        return if (time == window.end) TriggerResult.FIRE_AND_PURGE else TriggerResult.CONTINUE
    }
}
```

---

### 5. Evictors

Optional component to remove elements _inside_ the window operator before processing.

#### `Evictor` Interface

```kotlin
interface Evictor<T, W : Window> {
    // Called BEFORE the window function is applied
    fun evictBefore(elements: Iterable<TimestampedValue<T>>, size: Int, window: W, ctx: EvictorContext)

    // Called AFTER the window function is applied
    fun evictAfter(elements: Iterable<TimestampedValue<T>>, size: Int, window: W, ctx: EvictorContext)
}
```

#### Constraints & Performance

Using an Evictor forces Flink to store **all raw elements** in state, effectively disabling the memory benefits of `ReduceFunction` or `AggregateFunction` (though those functions will still run on the remaining elements).

---

### 6. Window Lifecycle & State Management

### Lifecycle Steps

1.  **Creation**: On the first element (`assignWindows`). `windowState` is initialized.
2.  **Processing**: Elements accumulate. Triggers set timers.
3.  **Firing**: Trigger returns `FIRE`. Function runs.
4.  **Lateness**: If `allowedLateness > 0`, window persists after `window.end`. Late elements trigger `onElement` again.
5.  **Purging (Deletion)**:
    - Happens when `watermark >= window.end + allowedLateness`.
    - **Flink cleans:** Window contents (elements/aggregation), Window Object.
    - **YOU must clean:** Any per-window state defined in a custom `Trigger` or `ProcessWindowFunction` (via the `clear()` method).

#### State Leakage Warning

If implementing a custom `Trigger`, implementing `clear()` is mandatory.

```kotlin
override fun clear(window: W, ctx: TriggerContext) {
    ctx.getPartitionedState(countStateDescriptor).clear() // Clean up custom state
    ctx.deleteEventTimeTimer(window.end) // Clean up timers
}
```

#### Global State vs. Window State

- **Window State (`context.windowState()`):** Isolated per window instance. Automatically cleared when window closes.
  - _Example:_ `PerWindowAverage`
- **Global State (`context.globalState()`):** Persists across windows for the same key. Must be managed manually.
  - _Example:_ `HistoricalAverage` (for comparing current window to history).

---

### 7. Handling Late Data

Flink provides a three-tiered strategy for late events (timestamp < current watermark).

1.  **Default (Drop):** If the watermark has passed `window.end`, the element is dropped.
2.  **`allowedLateness` (Update):**
    ```kotlin
    .allowedLateness(Duration.ofMinutes(10))
    ```
    - The window state is kept for 10 extra minutes.
    - Late elements trigger a new `FIRE` (computation updates).
    - **Important:** `ProcessWindowFunction` will see the _updated_ full list. `AggregateFunction` will just receive the new _updated_ result.
3.  **Side Output (collect):**
    ```kotlin
    val lateTag = OutputTag<MyEvent>("late-data")
    .sideOutputLateData(lateTag)
    ```
    - Data arriving _after_ allowed lateness is not dropped, but sent to this side stream.
