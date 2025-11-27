# State Management

This document provides a comprehensive overview of the state primitives available in Apache Flink. State is a critical component of any sophisticated stream processing application, allowing operators to remember information from past events. Flink provides a rich set of state primitives that are automatically checkpointed and managed by the runtime, ensuring fault tolerance. State is broadly categorized into **Keyed State** and **Operator State**.

## Table of Contents

- [Keyed State](#keyed-state)
  - [`ValueState<T>`](#valuestatet)
  - [`ListState<T>`](#liststatet)
  - [`MapState<K, V>`](#mapstatek-v)
  - [`ReducingState<T>`](#reducingstatet)
  - [`AggregatingState<IN, OUT>`](#aggregatingstatein-out)
- [Operator State](#operator-state)
  - [`ListState<T>`](#liststatet-operator-state)
  - [`BroadcastState<K, V>`](#broadcaststatek-v)

---

## Keyed State

Keyed State is the most common type of state. It is always associated with a specific key and can only be used in functions and operators applied on a `KeyedStream` (e.g., in a `KeyedProcessFunction`). Flink ensures that all state for a given key is managed together and processed by the same operator instance.

### `ValueState<T>`

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

### `ListState<T>`

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

### `MapState<K, V>`

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

### `ReducingState<T>`

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

### `AggregatingState<IN, OUT>`

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

---

## Operator State

Operator State is scoped to a parallel operator instance (or sub-task), not to a key. It is a good choice for state that is not tied to any specific key, and it is most commonly used in custom sources and sinks.

### `ListState<T>` (Operator State)

The most common type of operator state. The state for the entire operator is the logical union of the lists in all of its parallel instances.

- **How it Works:** Each parallel instance of an operator manages its own piece of the state as a list of elements. When the job is restored or rescaled, Flink provides different schemes for redistributing the state from the old instances' lists to the new instances.
  - **Even-Split Redistribution:** Each instance gets a sub-set of the total list. This is the default.
  - **Union Redistribution:** All instances get the complete list of all elements.
- **Use Case:** The classic example is a fault-tolerant Kafka source. Each parallel consumer instance uses `ListState` to store the topic partition offsets it is responsible for. Upon recovery, Flink reassigns these partition-offset pairs to the available consumer instances.

### `BroadcastState<K, V>`

A special type of operator state designed for the Broadcast State Pattern. It is a `Map` whose contents are guaranteed to be identical across all parallel instances of an operator.

- **How it Works:** You broadcast a low-volume "control" stream and connect it to your main data stream. In the processing function, you can write to the `BroadcastState` when processing elements from the control stream. This state is then available as read-only to all instances when processing elements from the main data stream.
- **Use Case:** Used to broadcast control data, such as a set of rules for fraud detection, a new machine learning model, or any configuration that needs to be applied consistently to all parallel tasks.

**Full Code Example**
This example uses `BroadcastState` to dynamically filter transactions based on a broadcasted set of blocked user IDs.

```kotlin
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector

data class Transaction(val userId: String, val amount: Double)
data class BlockRule(val userId: String, val block: Boolean)

class DynamicBlocker : KeyedBroadcastProcessFunction<String, Transaction, BlockRule, String>() {
    // Descriptor for the broadcast state. It must be consistent across tasks.
    private val ruleStateDescriptor = MapStateDescriptor("blockedUsers", String::class.java, Boolean::class.javaObjectType)

    // Process the main data stream
    override fun processElement(tx: Transaction, ctx: ReadOnlyContext, out: Collector<String>) {
        val rules = ctx.getBroadcastState(ruleStateDescriptor)
        if (rules.contains(tx.userId) && rules.get(tx.userId) == true) {
            out.collect("Blocked transaction for user: ${tx.userId}")
        } else {
            out.collect("Allowed transaction for user: ${tx.userId}")
        }
    }

    // Process the broadcast control stream
    override fun processBroadcastElement(rule: BlockRule, ctx: Context, out: Collector<String>) {
        val rules = ctx.getBroadcastState(ruleStateDescriptor)
        if (rule.block) {
            rules.put(rule.userId, true)
        } else {
            rules.remove(rule.userId)
        }
    }
}

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val transactions = env.fromElements(
        Transaction("user1", 10.0), Transaction("user2", 20.0),
        Transaction("user3", 30.0), Transaction("user1", 5.0)
    ).keyBy { it.userId }

    // Define the broadcast state descriptor
    val ruleStateDescriptor = MapStateDescriptor("blockedUsers", String::class.java, Boolean::class.javaObjectType)

    val rules = env.fromElements(
        BlockRule("user1", true), // Block user1
        BlockRule("user3", true), // Block user3
        BlockRule("user1", false) // Unblock user1
    ).broadcast(ruleStateDescriptor)

    transactions
        .connect(rules)
        .process(DynamicBlocker())
        .print()

    env.execute("BroadcastState Example")
}
```
