# Handling Late Data

In real-world stream processing, events often arrive out of order or delayed due to network latency, clock drift, or distributed sources. Flink provides sophisticated mechanisms to handle such scenarios, differentiating between "out-of-order" events (which arrive before the watermark and are handled by it) and truly "late" events, which arrive _after_ the watermark has passed their timestamp, indicating that their time-based window has already been processed.

The DataStream API offers three primary strategies for managing late events.

## Table of Contents

- [Dropping Late Events (Default)](#dropping-late-events-default)
- [Redirecting Late Events to a Side Output](#redirecting-late-events-to-a-side-output)
- [Updating Window Results with Allowed Lateness](#updating-window-results-with-allowed-lateness)
- [Custom Late Data Handling with Process Functions](#custom-late-data-handling-with-process-functions)

## Dropping Late Events (Default)

By default, Flink's window operators discard any event that is considered late. An event `e` is late if its timestamp is smaller than the current watermark (`e.timestamp < currentWatermark`).

- **Mechanism:** When a late event arrives at a window operator, the operator has already processed the window to which the event belongs. Since the window's state is purged after firing (by default), the late event is simply dropped.
- **Use Case:** This is suitable for applications where absolute accuracy is secondary to low latency and minimal state usage, such as real-time dashboarding or simple analytics.

## Redirecting Late Events to a Side Output

Instead of losing late data, you can redirect it to a separate stream using a **Side Output**. This is a non-destructive pattern that allows you to capture late events for logging, manual inspection, or reprocessing in a different pipeline.

### Syntax and Full Code Example

To use a side output, you define a unique `OutputTag` and apply it to the window operator using the `.sideOutputLateData()` method.

**API Signature:**

```kotlin
// In WindowedStream.java
public WindowedStream<T, K, W> sideOutputLateData(OutputTag<T> outputTag)
```

**Full Code Example:**

```kotlin
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.OutputTag
import java.time.Duration

data class SensorReading(val id: String, val timestamp: Long, val value: Double)

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    // 1. Define a unique tag for the side output stream
    val lateDataTag = OutputTag<SensorReading>("late-readings")

    val sourceStream: DataStream<SensorReading> = env.fromElements(
        SensorReading("sensor_1", 1000L, 10.0), // On-time
        SensorReading("sensor_1", 6000L, 12.0), // On-time for next window
        SensorReading("sensor_1", 4000L, 11.0)  // Late, will be redirected
    )

    val mainResultStream = sourceStream
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness<SensorReading>(Duration.ofSeconds(1))
                .withTimestampAssigner { element, _ -> element.timestamp }
        )
        .keyBy { it.id }
        .window(TumblingEventTimeWindows.of(Time.seconds(5))) // Windows are [0-5), [5-10), ...
        // 2. Configure the window to send late data to the tag
        .sideOutputLateData(lateDataTag)
        .sum("value")

    // 3. Retrieve the late stream from the main result stream
    val lateStream: DataStream<SensorReading> = mainResultStream.getSideOutput(lateDataTag)

    mainResultStream.print("Main Result >>")
    lateStream.printToErr("Late Data >>")

    env.execute("Side Output for Late Data")
}
```

In this example, the watermark will advance past the end of the `[0, 5000)` window. The event with timestamp `4000L` arrives after this and is redirected to the `lateStream`.

## Updating Window Results with Allowed Lateness

Flink can be configured to keep a window's state active for a specified duration after the watermark has passed its end. This `allowedLateness` period allows late events to be processed, triggering a re-computation of the window and emitting an updated result.

### The Update Lifecycle

1.  **First Firing:** The watermark passes the window's end time. The window function is executed, and an initial result is emitted.
2.  **Lateness Period:** The window and its state are **not** deleted. They are preserved for the duration of the allowed lateness.
3.  **Late Event Arrival:** If a late event arrives within this period, it is added to the window's state. The window function is executed again with the updated contents, emitting a new, refined result.
4.  **Final Purge:** Once the watermark exceeds `window_end + allowedLateness`, the window state is finally purged. Any subsequent events are either dropped or sent to a side output if configured.

### Syntax and Downstream Implications

**API Signature:**

```kotlin
// In WindowedStream.java
public WindowedStream<T, K, W> allowedLateness(Time lateness)
```

**Full Code Example:**

```kotlin
// ... (setup is the same as the previous example) ...
val resultStreamWithUpdates = sourceStream
    .assignTimestampsAndWatermarks(...)
    .keyBy { it.id }
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    // Keep window state for an extra 2 seconds
    .allowedLateness(Time.seconds(2))
    .sum("value")
```

**Important Consideration:** Using `allowedLateness` means your downstream systems must be prepared to handle **updates**. If a sink performs a simple `INSERT`, you will get duplicate records (the initial result and one or more updated results). The consuming system must be idempotent or use an `UPSERT`/`UPDATE` mechanism to handle these corrections.

## Custom Late Data Handling with Process Functions

The built-in `allowedLateness` and `sideOutputLateData` are features of the Window API. For more fine-grained control or custom logic outside of windows, you must handle lateness manually within a `KeyedProcessFunction`. This pattern gives you complete control over state and time.

### Implementation Pattern

1.  **Check Lateness:** In `processElement`, compare the element's timestamp against the current watermark from the `TimerService`.
2.  **Redirect if Late:** If `element.timestamp < ctx.timerService().currentWatermark()`, the element is late. Use `ctx.output()` to send it to a side output.
3.  **Process On-Time Data:** If the element is on time, process it, update state, and register event-time timers to perform actions in the future (e.g., to clean up state).
4.  **Clean Up State:** The `onTimer` callback is crucial for clearing state to prevent unbounded memory growth.

### Full Code Example

This example manually implements a timeout logic: if a user starts an action (`START` event) but doesn't finish it (`END` event) within 10 seconds, it flags the start event as "incomplete." Any `START` events that are already late are immediately sent to a side output.

```kotlin
data class UserEvent(val userId: String, val type: String, val timestamp: Long)

val lateEventTag = OutputTag<UserEvent>("late-starts")

class IncompleteActivityDetector : KeyedProcessFunction<String, UserEvent, String>() {
    private lateinit var startEventState: ValueState<UserEvent>

    override fun open(parameters: Configuration) {
        startEventState = runtimeContext.getState(ValueStateDescriptor("start-event", UserEvent::class.java))
    }

    override fun processElement(event: UserEvent, ctx: Context, out: Collector<String>) {
        val watermark = ctx.timerService().currentWatermark()

        // 1. Manually check if the event is late
        if (event.timestamp < watermark) {
            if (event.type == "START") {
                ctx.output(lateEventTag, event)
            }
            return // Ignore late END events
        }

        if (event.type == "START") {
            // If another START arrives, process the old one as incomplete first
            val previousStart = startEventState.value()
            if (previousStart != null) {
                out.collect("INCOMPLETE: User ${ctx.currentKey} started at ${previousStart.timestamp} but did not finish.")
            }
            startEventState.update(event)
            // Register a timer to check for completion
            ctx.timerService().registerEventTimeTimer(event.timestamp + 10000L)
        } else if (event.type == "END") {
            val startEvent = startEventState.value()
            if (startEvent != null) {
                out.collect("COMPLETE: User ${ctx.currentKey} finished in ${event.timestamp - startEvent.timestamp}ms.")
                // Clean up state and the timer
                ctx.timerService().deleteEventTimeTimer(startEvent.timestamp + 10000L)
                startEventState.clear()
            }
        }
    }

    override fun onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector<String>) {
        val startEvent = startEventState.value()
        // Timer fires only if an END event was not received in time
        if (startEvent != null && timestamp == startEvent.timestamp + 10000L) {
            out.collect("TIMEOUT: User ${ctx.currentKey} started at ${startEvent.timestamp} but did not finish within 10s.")
            startEventState.clear() // CRITICAL: Clean up state
        }
    }
}
```
