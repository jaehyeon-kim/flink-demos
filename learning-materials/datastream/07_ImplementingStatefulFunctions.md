# Implementing Stateful Flink Functions

This document provides a comprehensive guide to implementing stateful functions in Apache Flink. Stateful functions are the core of any non-trivial stream processing application, allowing them to remember information from past events to influence future processing. We will explore Flink's powerful state abstractions, including Keyed State, Operator State, and Broadcast State, with detailed explanations and best practices in Kotlin.

## Table of Contents

- [The Importance of State in Stream Processing](#the-importance-of-state-in-stream-processing)
- [Keyed State: The Most Common State Type](#keyed-state-the-most-common-state-type)
  - [Types of Keyed State](#types-of-keyed-state)
  - [Declaring and Using Keyed State](#declaring-and-using-keyed-state)
  - [Configuring State Time-To-Live (TTL)](#configuring-state-time-to-live-ttl)
- [Operator State: State Scoped to the Task](#operator-state-state-scoped-to-the-task)
  - [Understanding the CheckpointedFunction Interface](#understanding-the-checkpointedfunction-interface)
  - [Detailed Example: An Exactly-Once Buffering Sink](#detailed-example-an-exactly-once-buffering-sink)
- [Broadcast State: Dynamically Configure Your Application](#broadcast-state-dynamically-configure-your-application)
  - [The Broadcast State Pattern](#the-broadcast-state-pattern)
  - [Implementing a KeyedBroadcastProcessFunction](#implementing-a-keyedbroadcastprocessfunction)
- [Choosing the Right State Abstraction](#choosing-the-right-state-abstraction)

## The Importance of State in Stream Processing

Stateless applications process each event independently, without knowledge of previous events. While simple, this limits their capabilities. Stateful applications, on the other hand, can store and access data across events, enabling a wide range of complex operations:

- **Aggregations**: Calculating sums, counts, or averages over time windows.
- **Pattern Detection**: Identifying complex event sequences, such as fraud detection.
- **Machine Learning**: Maintaining and updating model parameters based on incoming data.
- **Data Enrichment**: Caching external data to enrich events without querying an external database for every record.

Flink provides first-class support for state, integrating it deeply with its checkpointing mechanism to guarantee fault tolerance and exactly-once processing semantics.

## Keyed State: The Most Common State Type

Keyed state is partitioned and scoped to a specific key. You can only use keyed state in operators applied on a `KeyedStream`, which is created via `dataStream.keyBy(...)`. This model is perfect for any logic that needs to be maintained on a per-entity basis, such as per-user activity, per-sensor readings, or per-transaction tracking. All state for a given key is managed by the same task instance.

### Types of Keyed State

Flink provides several primitives for keyed state, each designed for a different data structure.

- **`ValueState<T>`**: Stores a single value of type `T`. You can update it with `.update(T)` and retrieve it with `.value()`.
- **`ListState<T>`**: Stores a list of elements of type `T`. You can add elements with `.add(T)` or `.addAll(List<T>)`, retrieve all elements with `.get()`, and clear it with `.clear()`.
- **`MapState<UK, UV>`**: Stores a map of key-value pairs. You can add entries with `.put(UK, UV)`, retrieve values with `.get(UK)`, get all entries with `.entries()`, and remove entries with `.remove(UK)`.
- **`ReducingState<T>`**: Stores a single value that represents the aggregation of all values added to the state. It requires a `ReduceFunction` upon creation.
- **`AggregatingState<IN, OUT>`**: Similar to `ReducingState` but more general, allowing the input and output types to differ. It requires an `AggregateFunction`.

### Declaring and Using Keyed State

You declare and access keyed state within a `RichFunction` (like `RichMapFunction`) or, more commonly, a `ProcessFunction` (like `KeyedProcessFunction`). The state is accessed via the `RuntimeContext`.

#### `ValueState<T>`

Holds a single, updatable value of type `T`. It is the simplest and most common state primitive.

- **Use Case:** `ValueState` is the workhorse for most stateful logic. It's perfect for storing the "last seen" event, a running counter, the current state of a state machine, or the timestamp of a registered timer.
- **Key Methods:** `value()` to retrieve the current value, and `update(T)` to set or overwrite it.

**Interface Signature**

```kotlin
// Simplified for illustration
interface ValueState<T> {
    fun value(): T? // Returns null if not set
    fun update(value: T?)
    fun clear()
}
```

**Code Snippet**

```kotlin
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction

class MyProcessFunction : KeyedProcessFunction<String, MyEvent, String>() {
    // 1. Declare the state handle
    private lateinit var lastSeenState: ValueState<MyEvent>

    override fun open(parameters: Configuration) {
        // 2. Initialize the state handle
        val descriptor = ValueStateDescriptor("lastSeen", MyEvent::class.java)
        lastSeenState = runtimeContext.getState(descriptor)
    }

    override fun processElement(value: MyEvent, ctx: Context, out: Collector<String>) {
        val lastEvent = lastSeenState.value()
        // ... logic using lastEvent ...
        lastSeenState.update(value) // 3. Update the state
    }
}
```

**Full Code Example**
This example uses `ValueState` to detect when a sensor's temperature reading suddenly jumps by more than 10 degrees compared to its previous reading.

```kotlin
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

data class SensorReading(val id: String, val timestamp: Long, val temperature: Double)

class TemperatureJumpDetector : KeyedProcessFunction<String, SensorReading, String>() {
    private lateinit var lastTempState: ValueState<Double>

    override fun open(parameters: Configuration) {
        val descriptor = ValueStateDescriptor("lastTemperature", Double::class.javaObjectType)
        lastTempState = runtimeContext.getState(descriptor)
    }

    override fun processElement(reading: SensorReading, ctx: Context, out: Collector<String>) {
        val lastTemp = lastTempState.value()

        if (lastTemp != null && (reading.temperature - lastTemp).abs() > 10.0) {
            out.collect("ALERT for sensor '${ctx.currentKey}': Temp jump from $lastTemp to ${reading.temperature}")
        }

        // Update the state with the current temperature
        lastTempState.update(reading.temperature)
    }
}

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    val readings = env.fromElements(
        SensorReading("sensor_1", 1L, 20.0),
        SensorReading("sensor_1", 2L, 22.0),
        SensorReading("sensor_1", 3L, 35.0), // ALERT!
        SensorReading("sensor_2", 4L, 100.0),
        SensorReading("sensor_1", 5L, 34.0)
    )

    readings
        .keyBy { it.id }
        .process(TemperatureJumpDetector())
        .print()

    env.execute("ValueState Example")
}
```

#### `ListState<T>`

Holds a `List` of elements of type `T` for a given key.

- **Use Case:** Ideal for buffering or collecting a list of events that need to be processed together at a later time, for example, when a timer fires or a specific event arrives.
- **Key Methods:** `add(T)` to add a single element, `addAll(List<T>)` to add multiple, `get()` to retrieve the full list as an `Iterable<T>`, and `clear()` to remove all elements.

**Interface Signature**

```kotlin
// Simplified for illustration
interface ListState<T> {
    fun get(): Iterable<T>?
    fun add(value: T)
    fun addAll(values: List<T>)
    fun update(values: List<T>) // Replaces the entire list
    fun clear()
}
```

**Code Snippet**

```kotlin
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ListStateDescriptor

// ... inside a KeyedProcessFunction
private lateinit var bufferedEventsState: ListState<MyEvent>

override fun open(parameters: Configuration) {
    val descriptor = ListStateDescriptor("bufferedEvents", MyEvent::class.java)
    bufferedEventsState = runtimeContext.getListState(descriptor)
}

override fun processElement(value: MyEvent, ctx: Context, out: Collector<String>) {
    bufferedEventsState.add(value)
    if (bufferedEventsState.get().count() >= 10) {
        // Process the buffered events
        // ...
        bufferedEventsState.clear()
    }
}
```

**Full Code Example**
This example buffers up to 3 page visits for each user. Once the 3rd visit is recorded, it emits a summary of the visited pages for that user.

```kotlin
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

data class PageVisit(val userId: String, val url: String)

class VisitSummarizer : KeyedProcessFunction<String, PageVisit, String>() {
    private lateinit var pageHistoryState: ListState<String>

    override fun open(parameters: Configuration) {
        val descriptor = ListStateDescriptor("pageHistory", String::class.java)
        pageHistoryState = runtimeContext.getListState(descriptor)
    }

    override fun processElement(visit: PageVisit, ctx: Context, out: Collector<String>) {
        pageHistoryState.add(visit.url)
        val history = pageHistoryState.get().toList()

        if (history.size >= 3) {
            out.collect("User '${ctx.currentKey}' visited 3 pages: ${history.joinToString(" -> ")}")
            pageHistoryState.clear() // Clear state after processing
        }
    }
}

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    val visits = env.fromElements(
        PageVisit("user1", "/home"), PageVisit("user2", "/products"),
        PageVisit("user1", "/cart"), PageVisit("user2", "/about"),
        PageVisit("user1", "/checkout"), // Summary for user1 is emitted
        PageVisit("user2", "/contact")  // Summary for user2 is emitted
    )

    visits
        .keyBy { it.userId }
        .process(VisitSummarizer())
        .print()

    env.execute("ListState Example")
}
```

#### `MapState<K, V>`

Holds a `Map` of key-value pairs. It is the most flexible and general-purpose keyed state primitive.

- **Use Case:** Ideal for managing multiple related attributes per key. For example, counting occurrences of different sub-categories (the map key) for a given user (the stream key), or storing features of an item for fraud detection.
- **Key Methods:** `get(K)`, `put(K, V)`, `contains(K)`, `remove(K)`, `entries()`, `keys()`, `values()`.

**Interface Signature**

```kotlin
// Simplified for illustration
interface MapState<UK, UV> {
    fun get(key: UK): UV?
    fun put(key: UK, value: UV)
    fun contains(key: UK): Boolean
    fun remove(key: UK)
    fun entries(): Iterable<Map.Entry<UK, UV>>
    fun keys(): Iterable<UK>
    fun values(): Iterable<UV>
    fun isEmpty(): Boolean
    fun clear()
}
```

**Code Snippet**

```kotlin
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.common.state.MapStateDescriptor

// ... inside a KeyedProcessFunction
private lateinit var eventCountsState: MapState<String, Long>

override fun open(parameters: Configuration) {
    val descriptor = MapStateDescriptor("eventCounts", String::class.java, Long::class.javaObjectType)
    eventCountsState = runtimeContext.getMapState(descriptor)
}

override fun processElement(value: MyEvent, ctx: Context, out: Collector<String>) {
    val eventType = value.type
    val currentCount = eventCountsState.get(eventType) ?: 0L
    eventCountsState.put(eventType, currentCount + 1)
}
```

**Full Code Example**

This example counts the number of times each user performs different actions (e.g., 'view', 'click', 'purchase').

```kotlin
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

data class UserAction(val userId: String, val action: String)

class ActionCounter : KeyedProcessFunction<String, UserAction, String>() {
    private lateinit var actionCountState: MapState<String, Int>

    override fun open(parameters: Configuration) {
        val descriptor = MapStateDescriptor("actionCounts", String::class.java, Int::class.javaObjectType)
        actionCountState = runtimeContext.getMapState(descriptor)
    }

    override fun processElement(action: UserAction, ctx: Context, out: Collector<String>) {
        val currentCount = actionCountState.get(action.action) ?: 0
        actionCountState.put(action.action, currentCount + 1)

        val counts = actionCountState.entries().joinToString { "${it.key}: ${it.value}" }
        out.collect("User '${ctx.currentKey}' stats: [ $counts ]")
    }
}

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    val actions = env.fromElements(
        UserAction("user1", "view"), UserAction("user1", "click"),
        UserAction("user2", "view"), UserAction("user1", "view")
    )

    actions
        .keyBy { it.userId }
        .process(ActionCounter())
        .print()

    env.execute("MapState Example")
}
```

#### `ReducingState<T>`

Holds a single value of type `T`, similar to `ValueState`, but with a built-in aggregation mechanism.

- **How it Works:** You provide a `ReduceFunction` during its creation. Every time you add a new element via `.add(T)`, the state automatically combines the new element with its current value using your function. This is more efficient than the manual `get-update-write` pattern required with `ValueState`.
- **Use Case:** Best suited for simple, continuous aggregations like a rolling sum, min, max, or string concatenation.
- **Key Methods:** `add(T)` to add a new element and trigger the reduction, and `get()` to retrieve the current aggregated value.

**Interface Signature**

```kotlin
// Simplified for illustration
interface ReducingState<T> {
    fun get(): T?
    fun add(value: T)
    fun clear()
}
```

**Code Snippet**

```kotlin
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ReducingState
import org.apache.flink.api.common.state.ReducingStateDescriptor

// ... inside a KeyedProcessFunction
private lateinit var sumState: ReducingState<Long>

override fun open(parameters: Configuration) {
    val descriptor = ReducingStateDescriptor("sum", ReduceFunction { a, b -> a + b }, Long::class.javaObjectType)
    sumState = runtimeContext.getReducingState(descriptor)
}

override fun processElement(value: MyEvent, ctx: Context, out: Collector<String>) {
    sumState.add(value.amount)
    out.collect("Current sum for key ${ctx.currentKey}: ${sumState.get()}")
}
```

#### `AggregatingState<IN, OUT>`

The most general and flexible aggregation state. It holds a single value that is the result of an aggregation, but allows the input, intermediate accumulator, and output types to be different.

- **How it Works:** You provide a full `AggregateFunction`. This interface defines how to create an accumulator, how to add an input element to it, how to merge two accumulators, and how to get the final result.
- **Use Case:** For complex aggregations where the intermediate state is different from the input or output types. The canonical example is calculating a running average, where the accumulator must be a `(sum, count)` pair to be correct.
- **Key Methods:** `add(IN)` to add an element to the aggregation, and `get()` to retrieve the final result.

**Interface Signature**

```kotlin
// Simplified for illustration
interface AggregatingState<IN, OUT> {
    fun get(): OUT?
    fun add(value: IN)
    fun clear()
}
```

**Code Snippet**

```kotlin
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.AggregatingState
import org.apache.flink.api.common.state.AggregatingStateDescriptor

// ... inside a KeyedProcessFunction
private lateinit var averageState: AggregatingState<Double, Double>

override fun open(parameters: Configuration) {
    val descriptor = AggregatingStateDescriptor(
        "average",
        // The AggregateFunction with (sum, count) as accumulator
        object : AggregateFunction<Double, Pair<Double, Int>, Double> {
            override fun createAccumulator() = Pair(0.0, 0)
            override fun add(value: Double, acc: Pair<Double, Int>) = Pair(acc.first + value, acc.second + 1)
            override fun getResult(acc: Pair<Double, Int>) = acc.first / acc.second
            override fun merge(a: Pair<Double, Int>, b: Pair<Double, Int>) = Pair(a.first + b.first, a.second + b.second)
        },
        // Type information for the accumulator
        TypeInformation.of(new TypeHint<Pair<Double, Int>>() {})
    )
    averageState = runtimeContext.getAggregatingState(descriptor)
}

override fun processElement(value: MyEvent, ctx: Context, out: Collector<String>) {
    averageState.add(value.measurement)
    out.collect("Current average for key ${ctx.currentKey}: ${averageState.get()}")
}
```

**Best Practices:**

- **Initialize in `open()`**: State descriptors and handles should always be initialized in the `open()` method. This is the most efficient pattern.
- **Use Descriptive Names**: Give your state descriptors unique and meaningful names (e.g., `"user-session-start-time"`). This is critical for savepoint compatibility and debugging.

### Configuring State Time-To-Live (TTL)

To prevent state from growing indefinitely, you can configure a Time-To-Live (TTL). Flink will automatically clear state that has not been accessed for the configured duration.

```kotlin
import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.common.time.Time

val ttlConfig = StateTtlConfig
    .newBuilder(Time.hours(24)) // State will expire 24 hours after last access
    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite) // Reset TTL on read or write
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // Don't return expired state
    .build()

val descriptor = ValueStateDescriptor("myState", String::class.java)
descriptor.enableTimeToLive(ttlConfig) // Attach TTL config to the descriptor
```

## Operator State: State Scoped to the Task

Operator State is scoped to a parallel instance of an operator, not a key. Each parallel task maintains its own independent copy of the state. This is useful for implementing sources and sinks that need to remember information, such as Kafka connectors that track partition offsets or sinks that buffer records before writing them to an external system.

### Understanding the CheckpointedFunction Interface

This is the modern, recommended interface for implementing operator state.

**Interface Signature:**

```kotlin
interface CheckpointedFunction {
    // Called when Flink is taking a checkpoint. This is where you persist your local
    // state to Flink's managed state store.
    fun snapshotState(context: FunctionSnapshotContext)

    // Called once when the function is first initialized or when recovering from a failure.
    // This is where you get a handle to the managed state and restore your local variables.
    fun initializeState(context: FunctionInitializationContext)
}
```

### Detailed Example: An Exactly-Once Buffering Sink

This sink buffers a number of records in a local list before flushing them. It uses `CheckpointedFunction` to guarantee that no records are lost during a failure.

```kotlin
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.runtime.state.FunctionInitializationContext
import org.apache.flink.runtime.state.FunctionSnapshotContext
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

class BufferingSink(private val threshold: Int) : RichSinkFunction<String>(), CheckpointedFunction {
    // A handle to the managed, checkpointed operator state.
    @Transient
    private lateinit var checkpointedState: ListState<String>

    // The local, in-memory buffer for the current transaction. @Transient ensures it's not serialized.
    @Transient
    private val bufferedElements = mutableListOf<String>()

    override fun invoke(value: String, context: Context) {
        bufferedElements.add(value)
        if (bufferedElements.size >= threshold) {
            // In a real application, this would write to an external transactional system.
            println("Flushing buffer for subtask ${runtimeContext.indexOfThisSubtask}: $bufferedElements")
            bufferedElements.clear()
        }
    }

    override fun snapshotState(context: FunctionSnapshotContext) {
        // Before checkpointing, clear the old state and persist the current buffer content.
        checkpointedState.clear()
        for (element in bufferedElements) {
            checkpointedState.add(element)
        }
    }

    override fun initializeState(context: FunctionInitializationContext) {
        val descriptor = ListStateDescriptor("buffered-elements", String::class.java)
        // Get a handle to the ListState. Flink handles state redistribution on parallelism changes.
        checkpointedState = context.operatorStateStore.getListState(descriptor)

        // On recovery, restore any pending data from the last successful checkpoint into the local buffer.
        if (context.isRestored) {
            for (element in checkpointedState.get()) {
                bufferedElements.add(element)
            }
        }
    }
}
```

### Types of Operator State

#### `ListState<T>`

The most common type of operator state. The state for the entire operator is the logical union of the lists in all of its parallel instances.

- **How it Works:** Each parallel instance of an operator manages its own piece of the state as a list of elements. When the job is restored or rescaled, Flink provides different schemes for redistributing the state from the old instances' lists to the new instances.
  - **Even-Split Redistribution:** Each instance gets a sub-set of the total list. This is the default.
  - **Union Redistribution:** All instances get the complete list of all elements.
- **Use Case:** The classic example is a fault-tolerant Kafka source. Each parallel consumer instance uses `ListState` to store the topic partition offsets it is responsible for. Upon recovery, Flink reassigns these partition-offset pairs to the available consumer instances.

#### `UnionListState<T>`

This is an alternative to `ListState` with a different redistribution strategy.

- **How it Works:** Like `ListState`, each parallel instance manages a local list of state elements.
- **Redistribution on Rescale:**
  - **Union Redistribution:** On recovery, every instance receives a copy of the **complete list of all elements** from all other instances. It is up to the operator logic to figure out which parts of this duplicated state it should use.
- **Use Case:** This is useful when the operator needs a complete view of the global state to correctly initialize itself after a failure. For example, if the operator needs to recover routing information that must be consistent across all instances.

---

**Note on `ListCheckpointed`**: A simpler, now-deprecated interface called `ListCheckpointed` exists. For all new development, it is strongly recommended to use the more powerful and flexible `CheckpointedFunction` interface.

| Feature         | `ListCheckpointed` (Old)                                                           | `CheckpointedFunction` (New)                                                                     |
| :-------------- | :--------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------- |
| **State Scope** | Operator State only.                                                               | **Both Operator State and Keyed State.**                                                         |
| **State Types** | Limited to `ListState` by default.                                                 | Richer types via state stores (`ValueState`, `ListState`, `MapState`, etc.).                     |
| **Interface**   | `snapshotState`, `restoreState`.                                                   | `snapshotState`, `initializeState`.                                                              |
| **Flexibility** | Low. Only suitable for non-keyed, list-based state.                                | **High.** Provides a unified model for functions that need both keyed and operator state.        |
| **Rescaling**   | Restore logic must manually handle aggregating lists from multiple previous tasks. | Same logic required for operator state, but keyed state is redistributed automatically by Flink. |
| **Status**      | **Deprecated.**                                                                    | **Current and recommended approach.**                                                            |

In short, `CheckpointedFunction` is the successor to `ListCheckpointed`. It provides a more powerful and flexible model by giving the programmer access to both keyed and operator state stores within a single, unified interface.

---

## Broadcast State: Dynamically Configure Your Application

Broadcast State is a special type of operator state designed to solve a specific problem: making data from one stream available to all parallel instances of a downstream operator. This is the recommended pattern for dynamically broadcasting configuration, rules, or patterns to an entire operator.

> ❗ **Broadcast State is technically a special type of Operator State**. However, its API, behavior, and use cases are so distinct that it is almost always discussed and categorized as a third, top-level type

### `BroadcastState<K, V>`

This is a special type of operator state designed to support a specific pattern: broadcasting a stream of data to all parallel instances of an operator and allowing them to store it as a shared map.

- **How it Works:** It is a map-based state (`Map<K, V>`). The contents of `BroadcastState` are replicated and identical across all parallel instances of the operator. This ensures that every task has the same set of broadcasted data.
- **Redistribution on Rescale:** On recovery, Flink ensures every instance gets a copy of the full map state. Since it's already identical everywhere, this is straightforward.
- **Use Case:** Enrichment joins where a low-throughput "configuration" or "metadata" stream needs to be applied to a high-throughput data stream. For instance, broadcasting a stream of updated fraud detection rules to all transaction processing tasks. This pattern is implemented using a `BroadcastProcessFunction` or `KeyedBroadcastProcessFunction`.

### The Broadcast State Pattern

1.  **Control Stream**: A low-throughput stream containing the rules or configuration data.
2.  **Data Stream**: The main stream of data to be processed.
3.  **Broadcast**: The control stream is broadcasted using `.broadcast()`.
4.  **Connect**: The data stream is connected to the broadcasted stream using `.connect()`.
5.  **Process**: A `BroadcastProcessFunction` or `KeyedBroadcastProcessFunction` is applied to the connected streams to implement the application logic.

### Implementing a KeyedBroadcastProcessFunction

This function has two methods: one for handling elements from the regular stream and one for elements from the broadcast stream.

**Interface Signature (`KeyedBroadcastProcessFunction`):**

```kotlin
abstract class KeyedBroadcastProcessFunction<KS, IN1, IN2, O> {
    // Processes elements from the non-broadcasted, keyed stream.
    // Has READ-ONLY access to the broadcast state.
    abstract fun processElement(value: IN1, ctx: ReadOnlyContext, out: Collector<O>)

    // Processes elements from the broadcasted stream.
    // Has READ-WRITE access to the broadcast state.
    abstract fun processBroadcastElement(value: IN2, ctx: Context, out: Collector<O>)
}
```

**Kotlin Example: Dynamic Rule-Based Alerting**

```kotlin
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector

// Rule definition to be broadcasted
data class Rule(val ruleId: String, val pattern: String)
data class TelemetryEvent(val deviceId: String, val message: String)

class DynamicAlertingFunction(private val ruleStateDescriptor: MapStateDescriptor<String, Rule>) :
    KeyedBroadcastProcessFunction<String, TelemetryEvent, Rule, String>() {

    // Handles the main data stream
    override fun processElement(value: TelemetryEvent, ctx: ReadOnlyContext, out: Collector<String>) {
        // Iterate over all rules currently in the broadcast state (read-only)
        for (entry in ctx.getBroadcastState(ruleStateDescriptor).immutableEntries()) {
            val rule = entry.value
            if (value.message.contains(rule.pattern)) {
                out.collect("ALERT! Device ${value.deviceId} matched rule ${rule.ruleId}")
            }
        }
    }

    // Handles the broadcasted rule stream
    override fun processBroadcastElement(value: Rule, ctx: Context, out: Collector<String>) {
        // Update the broadcast state. This change is propagated to all tasks.
        ctx.getBroadcastState(ruleStateDescriptor).put(value.ruleId, value)
        println("New rule added/updated: ${value.ruleId}")
    }
}
```

**Best Practices:**

- **Low Throughput**: The broadcast stream should be low-volume, as every element is sent to every downstream task.
- **MapState Only**: Broadcast state is always a `MapState`.
- **Consistency**: Flink guarantees that all tasks will see the same broadcast state for events that arrive after a checkpoint that followed the broadcast update.

## Choosing the Right State Abstraction

| State Type          | Scope             | Common Use Case                                                 | Key Characteristics                                                                                             |
| :------------------ | :---------------- | :-------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------- |
| **Keyed State**     | Per Key           | Aggregations, state machines, per-entity logic (e.g., per-user) | Must be used on a `KeyedStream`. Rich set of primitives (`Value`, `List`, `Map`).                               |
| **Operator State**  | Per Task Instance | Source/Sink connectors (e.g., Kafka offsets), buffering data    | State is independent across parallel tasks. Managed via `CheckpointedFunction`.                                 |
| **Broadcast State** | Global (per task) | Dynamic configuration, broadcasting rules or patterns           | Connects a broadcast stream with a data stream. Read-only for data elements, read-write for broadcast elements. |
