# DataStream Transformations

This document provides a detailed overview of the transformation operations available in the Apache Flink DataStream API, with a focus on Kotlin examples. With the evolution of Flink, the DataStream API serves as the primary unified API for both batch and stream processing, following the deprecation of the DataSet API.

## Table of Contents

- [Basic Transformations (Element-wise)](#basic-transformations-element-wise)
  - [map](#map)
  - [filter](#filter)
  - [flatMap](#flatmap)
- [KeyedStream Transformations](#keyedstream-transformations)
  - [keyBy](#keyby)
  - [reduce](#reduce)
- [Multi-Stream Transformations](#multi-stream-transformations)
  - [union](#union)
  - [connect](#connect)
  - [Side Outputs](#side-outputs)
- [Distribution (Partitioning) Transformations](#distribution-partitioning-transformations)
  - [shuffle](#shuffle-random)
  - [rebalance](#rebalance-round-robin)
  - [rescale](#rescale)
  - [broadcast](#broadcast)
  - [global](#global)
  - [partitionCustom](#partitioncustom)

---

## Basic Transformations (Element-wise)

These transformations are applied to each element in a `DataStream` independently and are typically stateless.

### map

A 1-to-1 transformation that takes one element and produces exactly one transformed element. It is ideal for converting the data type, structure, or values of elements in a stream.

**Interface Signature**
The `map` transformation is applied to a `DataStream` and takes a `MapFunction` as an argument.

```kotlin
// Simplified for illustration
interface MapFunction<T, O> : Function {
    fun map(value: T): O
}

class DataStream<T> {
    fun <R> map(mapper: MapFunction<T, R>): DataStream<R>
}
```

**Code Snippet**

```kotlin
val inputStream: DataStream<Int> = // ...
val squaredStream: DataStream<Int> = inputStream.map { it * it }
```

**Full Code Example**

```kotlin
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream

data class SensorReading(val id: String, val timestamp: Long, val temperature: Double)

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val sensorData: DataStream<SensorReading> = env.fromElements(
        SensorReading("sensor_1", 1672531200L, 35.8),
        SensorReading("sensor_2", 1672531201L, 22.4)
    )

    // Map SensorReading objects to just their IDs
    val sensorIds: DataStream<String> = sensorData.map { it.id }

    sensorIds.print()

    env.execute("Map Example")
}
```

### filter

A 1-to-0/1 transformation. For each element, it evaluates a boolean condition. If the condition is true, the element is retained; otherwise, it is discarded.

**Interface Signature**
The `filter` transformation is applied to a `DataStream` and takes a `FilterFunction` as an argument.

```kotlin
// Simplified for illustration
interface FilterFunction<T> : Function {
    fun filter(value: T): Boolean
}

class DataStream<T> {
    fun filter(filter: FilterFunction<T>): DataStream<T>
}
```

**Code Snippet**

```kotlin
val inputStream: DataStream<Int> = // ...
val evenNumbersStream: DataStream<Int> = inputStream.filter { it % 2 == 0 }
```

**Full Code Example**

```kotlin
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream

data class SensorReading(val id: String, val timestamp: Long, val temperature: Double)

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val sensorData: DataStream<SensorReading> = env.fromElements(
        SensorReading("sensor_1", 1672531200L, 35.8),
        SensorReading("sensor_7", 1672531202L, 40.1), // High temperature
        SensorReading("sensor_2", 1672531201L, 22.4)
    )

    // Filter for readings with a temperature above 38 degrees
    val highTempReadings: DataStream<SensorReading> = sensorData.filter { it.temperature > 38.0 }

    highTempReadings.print()

    env.execute("Filter Example")
}
```

### flatMap

A 1-to-N transformation. For each input element, it can produce zero, one, or more output elements. This is useful for un-nesting elements, such as splitting a sentence into words.

**Interface Signature**
The `flatMap` transformation is applied to a `DataStream` and takes a `FlatMapFunction` as an argument, which uses a `Collector` to emit output elements.

```kotlin
// Simplified for illustration
interface FlatMapFunction<T, O> : Function {
    fun flatMap(value: T, out: Collector<O>)
}

class DataStream<T> {
    fun <R> flatMap(flatMapper: FlatMapFunction<T, R>): DataStream<R>
}
```

**Code Snippet**

````kotlin
val inputStream: DataStream<String> = // ... (stream of sentences)
val wordsStream: DataStream<String> = inputStream.flatMap { value, out ->
    value.split(" ").forEach { out.collect(it) }
}```

**Full Code Example**

```kotlin
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.util.Collector

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val sentences: DataStream<String> = env.fromElements(
        "Apache Flink is a powerful stream processor",
        "DataStream API is versatile"
    )

    // Split each sentence into words
    val words: DataStream<String> = sentences.flatMap { value: String, out: Collector<String> ->
        value.lowercase().split("\\W+".toRegex()).forEach { out.collect(it) }
    }

    words.print()

    env.execute("FlatMap Example")
}
````

---

## KeyedStream Transformations

These transformations are performed on a `KeyedStream`, which is a `DataStream` that has been partitioned by a key. All subsequent stateful operations are performed independently for each key.

### keyBy

The `keyBy` operator converts a `DataStream` into a `KeyedStream`. It repartitions the stream based on a key, ensuring that all elements with the same key are processed by the same task instance. This is a prerequisite for any keyed stateful operation.

**Interface Signature**

```kotlin
// Simplified for illustration
class DataStream<T> {
    fun <K> keyBy(keySelector: KeySelector<T, K>): KeyedStream<T, K>
}
```

**Code Snippet**

```kotlin
val inputStream: DataStream<SensorReading> = // ...
val keyedStream: KeyedStream<SensorReading, String> = inputStream.keyBy { it.id }
```

**Full Code Example**

```kotlin
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.KeyedStream

data class SensorReading(val id: String, val timestamp: Long, val temperature: Double)

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val sensorData: DataStream<SensorReading> = env.fromElements(
        SensorReading("sensor_1", 1672531200L, 35.8),
        SensorReading("sensor_2", 1672531201L, 22.4),
        SensorReading("sensor_1", 1672531202L, 36.1)
    )

    // Partition the stream by the sensor ID
    val keyedSensorData: KeyedStream<SensorReading, String> = sensorData.keyBy { it.id }

    keyedSensorData.print()

    env.execute("KeyBy Example")
}
```

### reduce

A rolling aggregation on a `KeyedStream`. It maintains a state for each key and combines the current state with each new element to produce a new state, which is then emitted downstream. It is used for implementing patterns like "rolling sums" or "finding running maximums" for each key.

**Interface Signature**
The `reduce` transformation is applied to a `KeyedStream` and takes a `ReduceFunction`.

```kotlin
// Simplified for illustration
interface ReduceFunction<T> : Function {
    fun reduce(value1: T, value2: T): T
}

class KeyedStream<T, K> {
    fun reduce(reducer: ReduceFunction<T>): DataStream<T>
}
```

**Code Snippet**

```kotlin
val keyedStream: KeyedStream<SensorReading, String> = // ...
val maxTempStream: DataStream<SensorReading> = keyedStream.reduce { r1, r2 ->
    if (r1.temperature > r2.temperature) r1 else r2
}
```

**Full Code Example**

```kotlin
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.api.common.functions.ReduceFunction

data class SensorReading(val id: String, val timestamp: Long, val temperature: Double)

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val sensorData: DataStream<SensorReading> = env.fromElements(
        SensorReading("sensor_1", 1672531200L, 35.8),
        SensorReading("sensor_1", 1672531201L, 36.4),
        SensorReading("sensor_2", 1672531202L, 22.4),
        SensorReading("sensor_1", 1672531203L, 36.1),
        SensorReading("sensor_2", 1672531204L, 23.1)
    )

    // Find the highest temperature reading per sensor, updated with each new event
    val maxTemperaturePerSensor: DataStream<SensorReading> = sensorData
        .keyBy { it.id }
        .reduce(ReduceFunction<SensorReading> { r1, r2 ->
            if (r1.temperature > r2.temperature) r1 else r2
        })

    maxTemperaturePerSensor.print()

    env.execute("Reduce Example")
}
```

---

## Multi-Stream Transformations

These operators combine or split multiple logical streams.

### union

Merges two or more streams of the **same data type** into a single output stream. The elements are interleaved, and Flink does not impose any specific order on the merged elements.

**Interface Signature**

```kotlin
// Simplified for illustration
class DataStream<T> {
    fun union(vararg streams: DataStream<T>): DataStream<T>
}
```

**Code Snippet**

```kotlin
val stream1: DataStream<Int> = // ...
val stream2: DataStream<Int> = // ...
val mergedStream: DataStream<Int> = stream1.union(stream2)
```

**Full Code Example**

```kotlin
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val streamA: DataStream<String> = env.fromElements("eventA1", "eventA2")
    val streamB: DataStream<String> = env.fromElements("eventB1", "eventB2")
    val streamC: DataStream<String> = env.fromElements("eventC1")

    // Union three streams of the same type
    val unifiedStream: DataStream<String> = streamA.union(streamB, streamC)

    unifiedStream.print()

    env.execute("Union Example")
}
```

### connect

Merges two streams of **potentially different data types**. The streams remain logically separate, allowing you to apply different logic to each. It is processed by a `CoProcessFunction` (or similar), which is ideal for applying logic based on a shared state between the two streams.

**Interface Signature**

```kotlin
// Simplified for illustration
class DataStream<T> {
    fun <T2> connect(dataStream: DataStream<T2>): ConnectedStreams<T, T2>
}
```

**Code Snippet**

```kotlin
val stream1: DataStream<Int> = // ...
val stream2: DataStream<String> = // ...
val connectedStream: ConnectedStreams<Int, String> = stream1.connect(stream2)
```

**Full Code Example**

```kotlin
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val controlStream: DataStream<String> = env.fromElements("BLOCK", "UNBLOCK")
    val dataStream: DataStream<String> = env.fromElements("data1", "data2", "data3")

    // Connect the data stream with a control stream
    val connectedStreams = dataStream.connect(controlStream.broadcast())

    val processedStream = connectedStreams.process(object : CoProcessFunction<String, String, String>() {
        private var isBlocked = false

        // Process elements from the first stream (data)
        override fun processElement1(value: String, ctx: Context, out: Collector<String>) {
            if (!isBlocked) {
                out.collect(value)
            }
        }

        // Process elements from the second stream (control)
        override fun processElement2(value: String, ctx: Context, out: Collector<String>) {
            isBlocked = (value == "BLOCK")
        }
    })

    processedStream.print()

    env.execute("Connect Example")
}

```

### Side Outputs

Splits a **single stream** into multiple streams based on some logic. Inside a `ProcessFunction`, you can emit data to a main output and multiple secondary "side" outputs. These side outputs can then be retrieved downstream as separate `DataStream`s.

**Interface Signature**
Side outputs are defined using an `OutputTag` and emitted from within a `ProcessFunction`.

```kotlin
// Simplified for illustration
class OutputTag<T>(val id: String)

abstract class ProcessFunction<I, O> {
    // Inside processElement method
    fun processElement(value: I, ctx: Context, out: Collector<O>) {
        ctx.output(outputTag, sideOutputValue)
    }
}
```

**Full Code Example**

```kotlin
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag

data class SensorReading(val id: String, val timestamp: Long, val temperature: Double)

val highTempTag = OutputTag<SensorReading>("high-temp") {}

class SplitterProcess : ProcessFunction<SensorReading, SensorReading>() {
    override fun processElement(value: SensorReading, ctx: Context, out: Collector<SensorReading>) {
        if (value.temperature > 38.0) {
            // Emit to side output for high temperatures
            ctx.output(highTempTag, value)
        } else {
            // Emit to main output for normal temperatures
            out.collect(value)
        }
    }
}

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val sensorData: DataStream<SensorReading> = env.fromElements(
        SensorReading("sensor_1", 1672531200L, 35.8),
        SensorReading("sensor_7", 1672531202L, 40.1),
        SensorReading("sensor_2", 1672531201L, 22.4)
    )

    val mainStream = sensorData.process(SplitterProcess())
    val highTempStream = mainStream.getSideOutput(highTempTag)

    mainStream.print("Normal")
    highTempStream.print("High")


    env.execute("Side Outputs Example")
}
```

---

## Distribution (Partitioning) Transformations

These transformations control how data is physically partitioned and sent from one parallel operator instance to another. They are crucial for managing parallelism and optimizing network traffic.

### shuffle (Random)

Distributes elements randomly and evenly to the downstream operator instances. This is useful when you want to ensure an even distribution of data to break data skew, but it does not preserve any element ordering.

**Code Snippet**

```kotlin
val inputStream: DataStream<String> = // ...
val shuffledStream: DataStream<String> = inputStream.shuffle()
```

### rebalance (Round-Robin)

Distributes elements in a round-robin fashion to the downstream instances. This guarantees an even workload and is often used to mitigate data skew when the cost of shuffling is a concern.

**Code Snippet**

```kotlin
val inputStream: DataStream<String> = // ...
val rebalancedStream: DataStream<String> = inputStream.rebalance()
```

### rescale

A more efficient, localized version of round-robin. It distributes elements only to a subset of downstream instances, minimizing network traffic. This is highly effective when the upstream and downstream operators have the same level of parallelism and a local data transfer is possible.

**Code Snippet**

```kotlin
val inputStream: DataStream<String> = // ...
val rescaledStream: DataStream<String> = inputStream.rescale()
```

### broadcast

Sends **every** element to **every** downstream operator instance. This is typically used for "control" streams where all tasks need the same information, such as rules, patterns, or configuration updates.

**Code Snippet**

````kotlin
val controlStream: DataStream<String> = // ...
val broadcastedStream: DataStream<String> = controlStream.broadcast()```

### global
Sends **all** elements to a **single** downstream operator instance (specifically, task instance 0). This forces a parallelism of 1 and creates a significant bottleneck, so it should be used with extreme caution.

**Code Snippet**
```kotlin
val inputStream: DataStream<String> = // ...
val globalStream: DataStream<String> = inputStream.global()
````

### partitionCustom

Allows you to implement a custom partitioning logic. You provide a `Partitioner` function that determines which downstream instance an element should be sent to, based on a key extracted from the element. `keyBy` is a highly optimized implementation of this pattern.

**Interface Signature**

```kotlin
// Simplified for illustration
interface Partitioner<K> {
    fun partition(key: K, numPartitions: Int): Int
}

class DataStream<T> {
    fun <K> partitionCustom(partitioner: Partitioner<K>, keySelector: KeySelector<T, K>): DataStream<T>
}
```

**Full Code Example**

```kotlin
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream

// Custom partitioner to send even numbers to partition 0 and odd numbers to partition 1
class EvenOddPartitioner : Partitioner<Int> {
    override fun partition(key: Int, numPartitions: Int): Int {
        return if (key % 2 == 0) 0 else 1
    }
}

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment().apply {
        parallelism = 2 // Ensure we have at least 2 partitions
    }

    val numbers: DataStream<Int> = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)

    val customPartitionedStream = numbers.partitionCustom(EvenOddPartitioner()) { it }

    customPartitionedStream.map { "Partition: ${it % 2}, Value: $it" }.print()

    env.execute("Custom Partitioning Example")
}
```
