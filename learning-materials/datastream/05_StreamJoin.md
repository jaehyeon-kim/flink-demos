# Joining Streams on Time

Apache Flink provides powerful operators to correlate events and join data streams based on time constraints. This is a critical capability for any real-time application that needs to combine information from different sources, such as matching order events with payment events, or enriching a transaction stream with user metadata.

## Table of Contents

- [Interval Join (Recommended)](#interval-join-recommended)
- [Window Join](#window-join)
- [CoGroup (Window Join Variant)](#cogroup-window-join-variant)
- [Advanced Custom Joins](#advanced-custom-joins)

## Interval Join (Recommended)

The Interval Join is the most flexible and efficient operator for joining streams based on a relative time interval. It matches elements from two keyed streams, `A` and `B`, where an element from `B` has a timestamp that falls within a specified time boundary relative to an element from `A`.

### Semantics

- **Logic:** Joins an element `a` from `streamA` with an element `b` from `streamB` if their keys match and their timestamps adhere to the condition: `a.timestamp + lowerBound <= b.timestamp <= a.timestamp + upperBound`.
- **Time Semantics:** This join operates exclusively in **Event Time**.
- **Join Type:** The native syntax supports only **INNER JOIN**. However, outer joins can be implemented manually within the `ProcessJoinFunction` by using timers and side outputs to handle unmatched elements.

### Syntax

**Interface Signature:**

```kotlin
// On a KeyedStream<T1, KEY>
fun <T2> intervalJoin(otherStream: KeyedStream<T2, KEY>): KeyedStream.IntervalJoined<T1, T2, KEY>

// IntervalJoined methods
fun between(lowerBound: Time, upperBound: Time): KeyedStream.IntervalJoined<T1, T2, KEY>
fun process(processJoinFunction: ProcessJoinFunction<T1, T2, R>): DataStream<R>
```

**Code Snippet:**

```kotlin
val streamA: KeyedStream<EventA, String> = //...
val streamB: KeyedStream<EventB, String> = //...

val joinedStream = streamA
    .intervalJoin(streamB)
    .between(Time.minutes(-5), Time.minutes(10)) // b.ts can be from 5 mins before to 10 mins after a.ts
    .process(MyProcessJoinFunction())
```

### `ProcessJoinFunction`

This function is called for every pair of elements that match the join condition. It provides access to the elements and contextual metadata.

**Interface Signature:**

```kotlin
public abstract class ProcessJoinFunction<IN1, IN2, OUT> {

    public abstract void processElement(
        IN1 left,
        IN2 right,
        Context ctx,
        Collector<OUT> out) throws Exception;

    public abstract class Context {
        public abstract long getTimestamp(); // The timestamp of the joined pair.
        public abstract long getLeftTimestamp(); // The timestamp of the left element of a joined pair.
        public abstract long getRightTimestamp(); // The timestamp of the right element of a joined pair.
        public abstract <X> void output(OutputTag<X> outputTag, X value);
    }
}
```

#### Internals: Buffering & Watermarks (Detailed Walkthrough)

To understand buffering, we must look at the join from the perspective of **State Retention**. Flink does not know if future events will arrive that match current events, so it must buffer _every_ element from _both_ sides until the time window for potential matches has completely passed.

**Scenario:**

- **Stream A (Orders):** `keyBy(orderId)`
- **Stream B (Shipments):** `keyBy(orderId)`
- **Requirement:** Match an Order with a Shipment if the Shipment occurs between **0 minutes** and **1 hour** after the Order.
- **Interval:** `between(Time.minutes(0), Time.minutes(60))`
  - `lowerBound = 0`
  - `upperBound = +60 min`

---

##### 1. The Buffering Logic

When an element arrives, it is immediately stored in a `MapState` (Timestamp -> List<Element>) and Flink checks the other stream's buffer for matches.

**Event 1: `Order A1` arrives at `12:00`**

- **Match Range:** Looking for Shipments between `[12:00, 13:00]`.
- **Action:**
  1.  **Buffer:** Store `A1` in the "Left State".
  2.  **Probe:** Check "Right State" for any Shipments in `[12:00, 13:00]`. (Assume none exist yet).

**Event 2: `Shipment B1` arrives at `12:30`**

- **Match Range:** Looking for Orders that _could have triggered_ this shipment.
  - Logic: `Order.ts + 0 <= Shipment.ts <= Order.ts + 60`
  - Reverse Logic: `Shipment.ts - 60 <= Order.ts <= Shipment.ts - 0`
  - Target Range: `[11:30, 12:30]`.
- **Action:**
  1.  **Buffer:** Store `B1` in the "Right State".
  2.  **Probe:** Check "Left State" for Orders in `[11:30, 12:30]`.
  3.  **Match:** Flink finds `Order A1` (12:00). It emits the pair `(A1, B1)`.

---

##### 2. The Cleanup Logic (Watermarks)

**Rule 1: Cleaning Up the Left Side (the `Order`)**

An `Order` arrives first. It needs to wait for a `Shipment` that can be up to 60 minutes _later_.

- **`Order A1` arrives at `12:00`.**
- Its "promise window" for matching Shipments is from `12:00` to `13:00` (`12:00 + 60 minutes`).
- Flink **cannot** delete `Order A1` until it is absolutely certain that no Shipment with a timestamp in the `[12:00, 13:00]` range will ever arrive.
- **When does Flink get this certainty?** When the **Watermark passes `13:00`**.
- A watermark of `13:01` is a guarantee that time has moved past the entire matching window for `Order A1`. At this point, `Order A1`'s promise has expired, and it is safely removed from state.

**Cleanup Rule for Left Element (A):** Delete when `Watermark > A.timestamp + upperBound`

**Rule 2: Cleaning Up the Right Side (the `Shipment`)**

A `Shipment` arrives. It needs to be matched against an `Order` that could have occurred up to 0 minutes _earlier_ (based on the `lowerBound` of 0).

- **`Shipment B1` arrives at `12:30`.**
- We need to find the window of `Order` timestamps that could match it. By reversing the logic (`Shipment.ts - 60 <= Order.ts <= Shipment.ts - 0`), we see that `Shipment B1` can match `Orders` from `11:30` to `12:30`.
- The **latest** possible `Order` it could match has a timestamp of `12:30`.
- Flink **cannot** delete `Shipment B1` until it is sure that no more `Orders` with a timestamp of `12:30` or earlier can arrive.
- **When does Flink get this certainty?** When the **Watermark passes `12:30`**.
- As shown in your table, when the watermark becomes `12:31`, Flink knows the window of opportunity for `Shipment B1` has closed. It is safely deleted.

**Cleanup Rule for Right Element (B):** Delete when `Watermark > B.timestamp - lowerBound`

| Time      | Event / Watermark     | State: Orders | State: Shipments | Why?                                                                                                                                     |
| :-------- | :-------------------- | :------------ | :--------------- | :--------------------------------------------------------------------------------------------------------------------------------------- |
| **12:00** | `Order A1 (12:00)`    | `[A1]`        | `[]`             | A1 is buffered. Its cleanup time is `12:00 + 60m = 13:00`.                                                                               |
| **12:30** | `Shipment B1 (12:30)` | `[A1]`        | `[B1]`           | B1 is buffered. Its cleanup time is `12:30 - 0m = 12:30`. **A match is found**, but neither's cleanup time has been reached.             |
| **12:45** | `W = 12:31`           | `[A1]`        | `[]`             | **B1 is removed.** The watermark `12:31` is greater than B1's cleanup time of `12:30`. A1's cleanup time (`13:00`) has not been reached. |
| **12:55** | `Order A2 (12:55)`    | `[A1, A2]`    | `[]`             | A2 is buffered. Its cleanup time is `12:55 + 60m = 13:55`.                                                                               |
| **13:01** | `W = 13:01`           | `[A2]`        | `[]`             | **A1 is removed.** The watermark `13:01` is greater than A1's cleanup time of `13:00`. A2's cleanup time has not been reached.           |

---

##### 3. The "Idle Source" Trap (State Explosion)

This is the most common production issue with Interval Joins.

**The Problem:**
Imagine `Order A1` is buffered, waiting for a watermark of `13:00` to be cleaned up.

- Your Kafka topic has 5 partitions.
- Partition 1-4 are active, producing events at `13:30`.
- **Partition 5 is silent (idle)**. It sent an event at `11:50` and hasn't sent anything since.

**The Consequence:**

- Flink's global watermark is the **minimum** of all source partitions' watermarks.
- `Global Watermark = min(13:30, 13:30, 13:30, 13:30, 11:50) = 11:50`.
- Because the watermark is stuck at `11:50`, it never reaches `13:00`.
- **Result:** `Order A1` (and `A2`, `A3`...) is **never deleted**. The state size grows indefinitely until the application crashes with `OutOfMemoryError`.

**The Solution:**
You **must** configure idleness detection in your `WatermarkStrategy`.

```kotlin
WatermarkStrategy
    .forBoundedOutOfOrderness<MyEvent>(Duration.ofSeconds(20))
    .withIdleness(Duration.ofMinutes(1)) // Treat a source as idle if it's quiet for 1 minute
```

This tells Flink: "If Partition 5 hasn't sent data for 1 minute, ignore it when calculating the global watermark." This allows the watermark to advance to `13:30`, triggering the cleanup of `Order A1`.

## Window Join

A Window Join groups elements from two streams into the same time window and then joins them. The primary drawback is its rigidity; elements that are very close in time but fall into different windows cannot be joined.

### Semantics

- **Logic:** Joins elements from two keyed streams that are assigned to the same window.
- **Boundary Problem:** An element at `12:59:59` in a one-hour tumbling window `[12:00, 13:00)` will **never** join with an element at `13:00:01`, as the second element belongs to the next window `[13:00, 14:00)`.

### Syntax and Examples

The syntax requires chaining `join()`, `where()`, `equalTo()`, and `window()` calls, followed by an `apply()` with a `JoinFunction`.

#### Tumbling Windows

Tumbling windows have a fixed size and do not overlap. They are useful for periodic reports, such as hourly statistics.

```kotlin
val streamA: DataStream<EventA> = //...
val streamB: DataStream<EventB> = //...

val joinedStream = streamA
    .join(streamB)
    .where { it.key }
    .equalTo { it.key }
    .window(TumblingEventTimeWindows.of(Time.hours(1)))
    .apply { left, right -> JoinedResult(left, right) }
```

#### Sliding Windows

Sliding windows have a fixed size but slide by a specified interval. They overlap if the slide interval is smaller than the window size, allowing for smoother, moving-average-style aggregations.

```kotlin
val streamA: DataStream<EventA> = //...
val streamB: DataStream<EventB> = //...

// A 1-hour window that slides every 10 minutes
val joinedStream = streamA
    .join(streamB)
    .where { it.key }
    .equalTo { it.key }
    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
    .apply { left, right -> JoinedResult(left, right) }
```

#### Session Windows

Session windows do not have a fixed size. Instead, they group elements by a period of inactivity, or a "session gap." They are ideal for user-centric analysis, such as tracking user sessions on a website.

```kotlin
val streamA: DataStream<EventA> = //...
val streamB: DataStream<EventB> = //...

// Group events into sessions with a 30-minute gap of inactivity
val joinedStream = streamA
    .join(streamB)
    .where { it.key }
    .equalTo { it.key }
    .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
    .apply { left, right -> JoinedResult(left, right) }
```

## CoGroup (Window Join Variant)

`coGroup` is a more general version of the window join. Instead of processing every pair of matched elements (a Cartesian product), it provides iterables of all elements from each stream within a given window. This makes it ideal for implementing outer joins or other set-based logic.

### Semantics

- **Execution:** The `CoGroupFunction` is called once per key, per window.
- **Use Case:** Excellent for implementing **outer joins**. If the iterable for one stream is empty, you can identify unmatched elements from the other stream.

### Syntax and Full Code Example (Left Join)

```kotlin
// Data classes
data class UserClick(val userId: String, val page: String, val timestamp: Long)
data class UserProfile(val userId: String, val name: String, val timestamp: Long)
data class EnrichedClick(val userName: String, val page: String)

// CoGroupFunction to perform a left join
class LeftJoinCoGroup : CoGroupFunction<UserClick, UserProfile, EnrichedClick> {
    override fun coGroup(
        clicks: Iterable<UserClick>,
        profiles: Iterable<UserProfile>,
        out: Collector<EnrichedClick>
    ) {
        // Since profile data is sparse, we expect one or zero profiles.
        val userName = profiles.firstOrNull()?.name ?: "Unknown User"

        for (click in clicks) {
            out.collect(EnrichedClick(userName, click.page))
        }
    }
}

// Stream processing logic
val clicks: DataStream<UserClick> = //...
val profiles: DataStream<UserProfile> = //...

val enrichedStream = clicks
    .coGroup(profiles)
    .where { it.userId }
    .equalTo { it.userId }
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .apply(LeftJoinCoGroup())

enrichedStream.print()
```

## Advanced Custom Joins

For join logic that doesn't fit the interval or window models, Flink's low-level process functions offer complete control over state and time.

### Stream-to-Stream Join with Timeout (`KeyedCoProcessFunction`)

This pattern is used to join two streams where a match is expected within a certain time frame. If no match occurs, state must be cleaned up to prevent memory leaks.

**Scenario:** Match a `Trade` event with its `Confirmation` event within 10 seconds.

**Strategy:**

1.  Connect the two keyed streams.
2.  When an element from either stream arrives, check if its counterpart is already in state.
3.  If a match is found, emit the joined result and clear the state.
4.  If no match is found, buffer the element in `ValueState` and register an event-time timer for 10 seconds in the future.
5.  In the `onTimer` method, clear the state to handle the timeout case, preventing state leaks.

**Full Code Example:**

```kotlin
data class Trade(val tradeId: String, val amount: Double, val timestamp: Long)
data class Confirmation(val tradeId: String, val status: String, val timestamp: Long)
data class MatchedTrade(val tradeId: String, val amount: Double, val status: String)

val unmatchedTradesTag = OutputTag<Trade>("unmatched-trades")

class TradeMatcher : KeyedCoProcessFunction<String, Trade, Confirmation, MatchedTrade>() {
    private lateinit var tradeState: ValueState<Trade>

    override fun open(parameters: Configuration) {
        tradeState = runtimeContext.getState(ValueStateDescriptor("trade-state", Trade::class.java))
    }

    override fun processElement1(trade: Trade, ctx: Context, out: Collector<MatchedTrade>) {
        // Buffer the trade and set a timer for cleanup
        tradeState.update(trade)
        ctx.timerService().registerEventTimeTimer(trade.timestamp + 10_000L)
    }

    override fun processElement2(conf: Confirmation, ctx: Context, out: Collector<MatchedTrade>) {
        val trade = tradeState.value()
        if (trade != null) {
            // Match found
            out.collect(MatchedTrade(trade.tradeId, trade.amount, conf.status))
            // Clean up state and timer
            tradeState.clear()
            ctx.timerService().deleteEventTimeTimer(trade.timestamp + 10_000L)
        }
        // If trade hasn't arrived, we can't join. We assume confirmations don't need buffering.
    }

    override fun onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector<MatchedTrade>) {
        val savedTrade = tradeState.value()
        if (savedTrade != null) {
            // Timer fired, meaning no confirmation arrived in time.
            ctx.output(unmatchedTradesTag, savedTrade)
            tradeState.clear()
        }
    }
}
```

### Enrichment Join (`BroadcastProcessFunction`)

This pattern is designed to enrich a high-throughput stream with data from a low-throughput "configuration" or "metadata" stream. The metadata stream is broadcast to all parallel instances of the operator, ensuring that every task has a local, up-to-date copy.

**Scenario:** Enrich a stream of user transactions with their current country of residence, where the country information changes infrequently.

**Strategy:**

1.  Define a `MapStateDescriptor` for the broadcast state.
2.  Broadcast the low-throughput `CountryUpdate` stream.
3.  Connect the high-throughput `Transaction` stream with the `BroadcastStream`.
4.  In the `processBroadcastElement` method, update the `MapState` with the latest country for each user.
5.  In the `processElement` method, access the broadcast state in a read-only fashion to enrich each transaction.

**Full Code Example:**

```kotlin
data class Transaction(val userId: String, val amount: Double)
data class CountryUpdate(val userId: String, val country: String)
data class EnrichedTransaction(val userId: String, val amount: Double, val country: String)

val countryStateDescriptor = MapStateDescriptor("country-rules", Types.STRING, Types.STRING)

val transactions: DataStream<Transaction> = //...
val countryUpdates: DataStream<CountryUpdate> = //...

val enrichedStream = transactions
    .keyBy { it.userId } // Keying is still good practice for logical partitioning
    .connect(countryUpdates.broadcast(countryStateDescriptor))
    .process(object : KeyedBroadcastProcessFunction<String, Transaction, CountryUpdate, EnrichedTransaction>() {
        override fun processElement(
            txn: Transaction,
            ctx: ReadOnlyContext,
            out: Collector<EnrichedTransaction>
        ) {
            val countryState = ctx.getBroadcastState(countryStateDescriptor)
            val country = countryState.get(txn.userId) ?: "Unknown"
            out.collect(EnrichedTransaction(txn.userId, txn.amount, country))
        }

        override fun processBroadcastElement(
            update: CountryUpdate,
            ctx: Context,
            out: Collector<EnrichedTransaction>
        ) {
            val countryState = ctx.getBroadcastState(countryStateDescriptor)
            countryState.put(update.userId, update.country)
        }
    })

enrichedStream.print()
```
