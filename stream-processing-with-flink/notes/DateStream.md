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
