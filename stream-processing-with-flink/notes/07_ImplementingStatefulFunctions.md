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

**Interface Signature (from `RuntimeContext`):**

```kotlin
interface RuntimeContext {
    fun <T> getState(stateDescriptor: ValueStateDescriptor<T>): ValueState<T>
    fun <T> getListState(stateDescriptor: ListStateDescriptor<T>): ListState<T>
    // ... and so on for other state types
}
```

**Detailed Kotlin Example:**

This `KeyedProcessFunction` computes session data for each user, storing the first event and the latest event in `ValueState`.

```kotlin
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

data class Event(val userId: String, val timestamp: Long, val data: String)
data class UserSession(val userId: String, val firstEventTime: Long, val lastEventTime: Long)

class SessionTracker : KeyedProcessFunction<String, Event, UserSession>() {
    private lateinit var firstEventState: ValueState<Long>
    private lateinit var lastEventState: ValueState<Long>

    override fun open(parameters: Configuration) {
        // 1. Create State Descriptors with unique names and type information.
        val firstEventDesc = ValueStateDescriptor("firstEventTime", Long::class.javaObjectType)
        val lastEventDesc = ValueStateDescriptor("lastEventTime", Long::class.javaObjectType)

        // 2. Get the state handle from the runtime context. This is done once per task.
        firstEventState = runtimeContext.getState(firstEventDesc)
        lastEventState = runtimeContext.getState(lastEventDesc)
    }

    override fun processElement(value: Event, ctx: Context, out: Collector<UserSession>) {
        val firstTime = firstEventState.value()
        val lastTime = lastEventState.value()

        if (firstTime == null) {
            // This is the first event for this user (key).
            firstEventState.update(value.timestamp)
        }
        // Always update the last event time.
        lastEventState.update(value.timestamp)

        // Output the current session state.
        out.collect(UserSession(value.userId, firstEventState.value(), lastEventState.value()))
    }
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

## Broadcast State: Dynamically Configure Your Application

Broadcast State is a special type of operator state designed to solve a specific problem: making data from one stream available to all parallel instances of a downstream operator. This is the recommended pattern for dynamically broadcasting configuration, rules, or patterns to an entire operator.

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
