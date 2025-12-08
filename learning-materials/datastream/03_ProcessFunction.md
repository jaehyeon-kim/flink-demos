# Process Function

The `ProcessFunction` is one of the most powerful, low-level operators in Apache Flink's DataStream API. It provides developers with direct access to the fundamental building blocks of any stateful streaming application: **state**, **timers**, and **side outputs**. You should use it when standard transformations like `map`, `filter`, or windowing are not expressive enough to implement your custom business logic. It processes elements one by one, offering fine-grained control over both data and time.

## Table of Contents

- [Key Features and Components](#key-features-and-components)
  - [State Management](#state-management)
  - [Timers and TimerService](#timers-and-timerservice)
  - [Emitting to Side Outputs](#emitting-to-side-outputs)
- [A Complete Example: Combining State, Timers, and Side Outputs](#a-complete-example-combining-state-timers-and-side-outputs)
- [The ProcessFunction Family](#the-processfunction-family)
  - [`KeyedProcessFunction` (Most Common)](#keyedprocessfunction-most-common)
  - [`ProcessFunction` (Non-Keyed)](#processfunction-non-keyed)
  - [`CoProcessFunction` (Non-Keyed, Connected)](#coprocessfunction-non-keyed-connected)
  - [`KeyedCoProcessFunction` (Keyed, Connected)](#keyedcoprocessfunction-keyed-connected)
  - [`ProcessWindowFunction`](#processwindowfunction)
  - [`BroadcastProcessFunction` & `KeyedBroadcastProcessFunction`](#broadcastprocessfunction--keyedbroadcastprocessfunction)

---

## Key Features and Components

### State Management

A `ProcessFunction` can be stateful. When applied on a `KeyedStream`, the state is automatically partitioned by key, meaning each key has its own independent state managed by Flink.

- **How it works:** You declare state objects (e.g., `ValueState`, `MapState`, `ListState`) as class members and initialize them in the `open()` method using the `RuntimeContext`. Flink automatically handles the checkpointing and recovery of this state.
- **Use Case:** Storing information required to correlate events over time, such as tracking the last seen value, maintaining a running count for a specific key, or implementing a complex state machine for fraud detection.

**Interface Signature (for `ValueState`)**

```kotlin
// Simplified for illustration
// This is obtained from the RuntimeContext
interface ValueState<T> {
    fun value(): T
    fun update(value: T)
    fun clear()
}

// In your ProcessFunction class
abstract class KeyedProcessFunction<K, I, O> : AbstractRichFunction() {
    // State is typically declared as a member
    @Transient
    private var myState: ValueState<MyStateObject>? = null

    override fun open(parameters: Configuration) {
        val descriptor = ValueStateDescriptor("myStateName", MyStateObject::class.java)
        myState = runtimeContext.getState(descriptor)
    }
}
```

**Code Snippet**

```kotlin
// Detect if a sensor's temperature is continuously rising
class TempIncreaseAlertFunction : KeyedProcessFunction<String, SensorReading, String>() {
    // Store the last seen temperature for the current key
    private lateinit var lastTempState: ValueState<Double>

    override fun open(parameters: Configuration) {
        val lastTempDescriptor = ValueStateDescriptor("lastTemp", Types.DOUBLE)
        lastTempState = runtimeContext.getState(lastTempDescriptor)
    }

    override fun processElement(value: SensorReading, ctx: Context, out: Collector<String>) {
        val lastTemp = lastTempState.value() ?: 0.0

        // Check for a significant temperature jump
        if (lastTemp > 0.0 && value.temperature > lastTemp + 10) {
            out.collect("ALERT on sensor ${ctx.currentKey}: Temperature jumped from $lastTemp to ${value.temperature}")
        }

        // Update the state with the current temperature
        lastTempState.update(value.temperature)
    }
}
```

### Timers and TimerService

The `TimerService` is a core feature that allows you to register callbacks ("timers") to be executed at a specific time in the future. Timers are always scoped to the current key and are **only available on keyed streams**. You access the `TimerService` via the `Context` object passed to the `processElement` and `onTimer` methods.

When a timer fires, the special callback method `onTimer()` is invoked, where you can implement your time-based logic.

**`TimerService` API**

- `currentProcessingTime(): Long`: Returns the current wall-clock time of the machine executing the operator.
- `currentWatermark(): Long`: Returns the timestamp of the current watermark. This represents the application's event-time progress and is the primary way to reason about lateness.
- `registerProcessingTimeTimer(timestamp: Long)`: Registers a timer for the current key. `onTimer()` will be called when the machine's processing time reaches the provided `timestamp`.
- `registerEventTimeTimer(timestamp: Long)`: Registers a timer for the current key. `onTimer()` will be called when the stream's watermark passes the timer's `timestamp`.
- `deleteProcessingTimeTimer(timestamp: Long)`: Deletes a previously registered processing-time timer for the current key.
- `deleteEventTimeTimer(timestamp: Long)`: Deletes a previously registered event-time timer for the current key.

**Interface Signature**

```kotlin
// Simplified for illustration
abstract class KeyedProcessFunction<K, I, O> : AbstractRichFunction() {
    // Method called for each element
    abstract fun processElement(value: I, ctx: Context, out: Collector<O>)

    // Callback method for when a timer fires
    abstract fun onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector<O>)

    // Context objects provide access to the TimerService
    abstract class Context {
        abstract fun timerService(): TimerService
        // ...
    }
}
```

### Emitting to Side Outputs

A `ProcessFunction` can emit data to multiple output streams, not just the main one. This is the modern, type-safe way to split or route a stream based on complex logic.

- **How it works:**
  1.  Define a static `OutputTag<T>` to identify the side stream with a unique name and data type.
  2.  Inside `processElement()` or `onTimer()`, use `ctx.output(myTag, data)` to send data to that specific side stream.
  3.  The main output stream is still populated via `out.collect(data)`.
- **Use Case:** A common pattern is to send valid, successfully processed data to the main output while routing malformed events, late data, errors, or alerts to a side output for separate logging, monitoring, or reprocessing.

---

## A Complete Example: Combining State, Timers, and Side Outputs

This example identifies sensors that have become inactive. If a sensor does not send a reading for 5 seconds (event time), a timer fires and sends a notification to a side output.

```kotlin
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag
import java.time.Duration

data class SensorReading(val id: String, val timestamp: Long, val temperature: Double)

// 1. Define the OutputTag for the side output stream
val inactiveSensorTag = OutputTag<String>("inactive-sensors")

// This function monitors sensor readings and flags inactive sensors
class InactiveSensorDetector : KeyedProcessFunction<String, SensorReading, SensorReading>() {

    // State to store the timestamp of the last registered timer
    private lateinit var lastTimerState: ValueState<Long>

    override fun open(parameters: Configuration) {
        val stateDescriptor = ValueStateDescriptor("lastTimer", Types.LONG)
        lastTimerState = runtimeContext.getState(stateDescriptor)
    }

    override fun processElement(reading: SensorReading, ctx: Context, out: Collector<SensorReading>) {
        // A. If a timer is already registered, delete it because we received a new reading
        val lastTimer = lastTimerState.value()
        if (lastTimer != null) {
            ctx.timerService().deleteEventTimeTimer(lastTimer)
        }

        // B. Register a new timer for 5 seconds in the future (event time)
        val newTimerTimestamp = ctx.timestamp() + 5000L
        ctx.timerService().registerEventTimeTimer(newTimerTimestamp)
        lastTimerState.update(newTimerTimestamp)

        // C. Forward the valid reading to the main output
        out.collect(reading)
    }

    override fun onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector<SensorReading>) {
        // D. Timer fired, meaning no reading was received for 5 seconds.
        //    Emit an alert to the side output.
        val alertMessage = "Sensor '${ctx.currentKey}' has been inactive for 5 seconds at time $timestamp"
        ctx.output(inactiveSensorTag, alertMessage)

        // Clean up state to prevent the timer from being deleted if a late event arrives
        lastTimerState.clear()
    }
}


fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val sensorData = env.fromElements(
        SensorReading("sensor_1", 1000L, 35.0),
        SensorReading("sensor_2", 2000L, 36.0),
        SensorReading("sensor_1", 6000L, 37.0), // sensor_1 is active
        SensorReading("sensor_1", 12000L, 38.0) // sensor_2 will be inactive by now
    ).assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness<SensorReading>(Duration.ofSeconds(1))
            .withTimestampAssigner { event, _ -> event.timestamp }
    )

    val processedStream = sensorData
        .keyBy { it.id }
        .process(InactiveSensorDetector())

    // Get the side output stream using the OutputTag
    val inactiveAlerts = processedStream.getSideOutput(inactiveSensorTag)

    processedStream.print("Main Output (Active)")
    inactiveAlerts.print("Side Output (Inactive Alerts)")

    env.execute("Inactive Sensor Detection")
}
```

---

## The ProcessFunction Family

Flink provides several variants of the `ProcessFunction` to match different stream types and use cases.

### `KeyedProcessFunction` (Most Common)

- **Operates on:** `KeyedStream`.
- **Key Features:**
  - Has access to **keyed state** (state is scoped to the key).
  - Has full access to the **`TimerService`** (timers are also scoped to the key).
  - Can emit to side outputs.
- **Use Case:** The workhorse for most complex event-driven logic, stateful event correlation, and custom time-based operations. The example above uses a `KeyedProcessFunction`.

### `ProcessFunction` (Non-Keyed)

- **Operates on:** `DataStream` (non-keyed).
- **Key Features:**
  - Can use **operator state**, but **no keyed state**.
  - **No access to the `TimerService`**. Timers require a key.
  - Can emit to side outputs.
- **Use Case:** Simple logic on a non-keyed stream that requires access to operator state or side outputs, such as counting all events passing through an operator instance.

### `CoProcessFunction` (Non-Keyed, Connected)

- **Operates on:** A non-keyed `ConnectedStreams` (from `streamA.connect(streamB)`).
- **Key Features:**
  - Has two distinct processing methods: `processElement1()` and `processElement2()`, one for each input stream.
  - Can use operator state, but **no keyed state**.
  - **No access to timers**.
- **Use Case:** Applying logic to two non-keyed streams, often involving a broadcast stream where one stream updates operator state that the other stream reads.

### `KeyedCoProcessFunction` (Keyed, Connected)

- **Operates on:** A `ConnectedStreams` where both streams are keyed by the same key type (`streamA.keyBy(...).connect(streamB.keyBy(...))`).
- **Key Features:**
  - Has two `processElement` methods that share access to the same **keyed state** and **`TimerService`**.
  - State and timers are scoped to the common key.
- **Use Case:** Ideal for implementing complex interactions between two keyed streams, such as enriching a stream of transactions with user profile updates from another stream.

**Full Code Example (`KeyedCoProcessFunction`)**

```kotlin
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector

data class Transaction(val userId: String, val amount: Double)
data class UserProfile(val userId: String, val name: String, val country: String)
data class EnrichedTransaction(val userId: String, val userName: String, val country: String, val amount: Double)

class TransactionEnricher : KeyedCoProcessFunction<String, Transaction, UserProfile, EnrichedTransaction>() {
    private lateinit var userProfileState: ValueState<UserProfile>

    override fun open(parameters: Configuration) {
        userProfileState = runtimeContext.getState(ValueStateDescriptor("userProfile", UserProfile::class.java))
    }

    // Process transactions from the first input stream
    override fun processElement1(tx: Transaction, ctx: Context, out: Collector<EnrichedTransaction>) {
        val profile = userProfileState.value()
        if (profile != null) {
            out.collect(EnrichedTransaction(tx.userId, profile.name, profile.country, tx.amount))
        } else {
            // Optionally, handle transactions for which no profile has arrived yet
        }
    }

    // Process profile updates from the second input stream
    override fun processElement2(profile: UserProfile, ctx: Context, out: Collector<EnrichedTransaction>) {
        // Update the state with the latest profile information
        userProfileState.update(profile)
    }
}

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val transactions = env.fromElements(
        Transaction("user1", 100.0),
        Transaction("user2", 250.0),
        Transaction("user1", 50.0) // Second transaction for user1
    )

    val profiles = env.fromElements(
        UserProfile("user1", "Alice", "USA"),
        UserProfile("user2", "Bob", "Germany")
    )

    val enriched = transactions.keyBy { it.userId }
        .connect(profiles.keyBy { it.userId })
        .process(TransactionEnricher())

    enriched.print()

    env.execute("KeyedCoProcessFunction Example")
}
```

### `ProcessWindowFunction`

- **Operates on:** `WindowedStream`.
- **Key Features:**
  - Its `process` method is called **once per window** when the window triggers.
  - Receives an `Iterable` containing all elements in the window.
  - Has access to per-window state and global state, but **no `TimerService`**. Timers are managed by the windowing mechanism itself.
- **Use Case:** Performing custom calculations on all elements in a window when a simple `reduce` or `aggregate` is not sufficient, often for collecting all events before processing.

### `BroadcastProcessFunction` & `KeyedBroadcastProcessFunction`

- **Operates on:** A stream connected to a `BroadcastStream`.
- **Key Features:**
  - Specialized functions for the Broadcast State Pattern.
  - They process a regular (keyed or non-keyed) stream and a broadcast stream.
  - The broadcast stream updates a special "broadcast state" that is replicated to all parallel instances of the operator.
- **Use Case:** Applying rules or patterns that are dynamically updated to all events in a stream. For example, dynamically updating a set of fraudulent user IDs from a control stream and checking all transactions against this broadcasted set.
