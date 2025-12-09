# Performance and Robustness of Stateful Applications

This document provides a detailed guide to configuring and managing state in Apache Flink for optimal performance, scalability, and long-term robustness. We will cover how to choose the right state backend, select efficient state primitives, and prevent state from growing indefinitely, with all code examples in Kotlin.

## Table of Contents

- [Choosing a State Backend](#choosing-a-state-backend)
  - [State Backend Options](#state-backend-options)
  - [Configuring a State Backend](#configuring-a-state-backend)
  - [Support for Object Storage (S3, HDFS)](#support-for-object-storage-s3-hdfs)
- [Choosing a State Primitive](#choosing-a-state-primitive)
  - [Impact of Serialization](#impact-of-serialization)
  - [State Primitives and Access Patterns](#state-primitives-and-access-patterns)
- [Preventing Leaking State](#preventing-leaking-state)
  - [Using State Time-To-Live (TTL)](#using-state-time-to-live-ttl)
  - [Programmatic Cleanup with Process Timers](#programmatic-cleanup-with-process-timers)

## Choosing a State Backend

In Apache Flink, a "State Backend" determines how the application's working state is stored. Flink separates this from **Checkpoint Storage**, which defines where durable snapshots (checkpoints) are written.

### State Backend Options

There are two primary, recommended state backends to choose from, each designed for different scalability needs.

#### 1. `HashMapStateBackend` (In-Memory State)

This backend keeps all of its working state as objects on the Java Virtual Machine (JVM) heap of the TaskManagers.

- **Working Storage**: On the JVM heap.
- **Checkpoint Storage**: Writes snapshots to a durable, remote file system (like Amazon S3, HDFS, or Google Cloud Storage).
- **Performance**: **Highest performance.** State access is as fast as reading from memory, avoiding any serialization overhead.
- **Limitations**: State size is strictly limited by the available TaskManager heap memory. It is not suitable for large states and can lead to Garbage Collection (GC) pressure.
- **Best For**:
  - Local development and testing.
  - Production jobs with very small and predictable state.

> **Note:** This backend, when combined with checkpointing to a file system, provides the exact functionality of the legacy `FileSystemStateBackend`, which is now functionally deprecated.

#### 2. `EmbeddedRocksDBStateBackend` (On-Disk State)

This backend manages its working state within an embedded RocksDB database, which stores its files on the TaskManager's local disk.

- **Working Storage**: On the local disk of the TaskManager, managed by RocksDB.
- **Checkpoint Storage**: Writes snapshots to a durable, remote file system, just like the `HashMapStateBackend`.
- **Performance**: Slightly slower for raw state access compared to the in-memory backend due to the need for serialization/deserialization on every read and write.
- **Scalability**: **Extremely high.** This is the only backend that can manage state far larger than the available memory (terabytes per TaskManager is possible). It also significantly reduces pressure on the JVM's Garbage Collector.
- **Key Feature**: It is the only backend that supports **incremental checkpoints**, which can dramatically reduce checkpointing time for large-state applications.
- **Best For**:
  - The standard for most production applications.
  - Any job with large state (e.g., large windows, complex keyed state).
  - Applications requiring high fault tolerance with minimal checkpointing overhead.

#### Summary: Which Backend Should You Use?

| Feature                     | `HashMapStateBackend`         | `EmbeddedRocksDBStateBackend`                         |
| :-------------------------- | :---------------------------- | :---------------------------------------------------- |
| **Working State Location**  | JVM Heap (In-Memory)          | Local Disk                                            |
| **Max State Size**          | Limited by RAM                | Limited by Disk Size                                  |
| **Performance**             | **Highest** (for small state) | **Excellent** (with better stability for large state) |
| **Incremental Checkpoints** | No                            | **Yes**                                               |
| **Primary Use Case**        | Small state, testing          | **Production standard, large state**                  |

### Configuring a State Backend

You configure the state backend in your Flink job or cluster configuration.

**Kotlin Code Snippet (configuring in the job):**

```kotlin
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    // --- Example: Configuring the RocksDBStateBackend ---
    // The boolean flag enables incremental checkpoints.
    val rocksDbBackend = RocksDBStateBackend("hdfs:///flink/checkpoints", true)

    env.stateBackend = rocksDbBackend

    // ... your application logic ...
}
```

**Flink configuration (flink-conf.yaml)**

**HashMapStateBackend**

```yaml
# 1. State Backend: Use 'hashmap' to store the working state in memory (JVM heap).
state.backend: hashmap

# 2. Checkpoint Storage: Define a directory on a durable filesystem (like S3, HDFS, or GCS).
# Flink will automatically use the FileSystemCheckpointStorage when this is set.
state.checkpoints.dir: s3a://flink-checkpoints/checkpoints/

# 3. Savepoint Directory: This configuration remains the same.
state.savepoints.dir: s3a://flink-savepoints/savepoints/
```

**EmbeddedRocksDBStateBackend**

```yaml
# =============================================================================
#  Essential State Backend Configuration
# =============================================================================

# 1. Enable the RocksDB state backend.
state.backend: rocksdb

# 2. Set the remote, durable directory for automatic checkpoints (fault tolerance).
state.checkpoints.dir: s3a://flink-checkpoints/checkpoints/

# 3. Set a SEPARATE remote directory for user-managed savepoints (operational actions).
state.savepoints.dir: s3a://flink-checkpoints/savepoints/

# 4. Point RocksDB's working files to a dedicated local SSD for high performance.
state.backend.rocksdb.localdir: /data/flink/rocksdb

# =============================================================================
#  Performance and Checkpointing Tuning
# =============================================================================

# 5. Enable incremental checkpoints to drastically reduce checkpoint times for large state.
state.backend.rocksdb.incremental: true

# 6. Use a predefined profile optimized for modern SSDs.
state.backend.rocksdb.predefined-options: FLASH_SSD_OPTIMIZED

# 7. Increase the number of background threads for compaction and flushing.
state.backend.rocksdb.thread.num: 4

# =============================================================================
#  Memory Management
# =============================================================================

# 8. Allocate 40% of the TaskManager's memory to be managed by Flink for state.
taskmanager.memory.managed.fraction: 0.4

# 9. Ensure Flink's managed memory control for RocksDB is enabled (default and recommended).
state.backend.rocksdb.memory.managed: true
```

### Support for Object Storage (S3, HDFS)

Flink has first-class support for object storage like S3, Google Cloud Storage, and HDFS. This is configured as the destination for **checkpoints and savepoints**, not directly in the state backend itself (though the path is often passed to the backend's constructor).

Flink uses filesystem plugins to communicate with these systems. For example, to use S3, you would:

1.  Add the `flink-s3-fs-hadoop` or `flink-s3-fs-presto` dependency to your project.
2.  Place the plugin JAR in Flink's `/plugins` directory.
3.  Configure checkpoint paths with an `s3://` prefix.

## Choosing a State Primitive

The way you structure your state has a significant impact on performance, primarily due to serialization and deserialization overhead.

### Impact of Serialization

Flink must serialize state for two reasons:

1.  To write it to checkpoints (for all backends).
2.  To write it to disk (for `RocksDBStateBackend` on every state access).

Efficient serialization is key to low-latency processing. Flink's serialization stack is highly optimized.

**Best Practices for Performant Serialization:**

- **Use Simple Types**: Prefer primitive types (Long, Double, etc.) and simple POJOs (Plain Old Java Objects) or Kotlin data classes. Flink has highly efficient serializers for these.
- **Avoid Generic Types**: Avoid using generic types like `Object` or `Tuple` if possible, as they often fall back to the slower Kryo serializer.
- **POJO/Data Class Rules**: Ensure your data classes are public, have a public constructor, and all fields are either public or have getters/setters.
- **Register Custom Serializers**: If you must use complex types, consider registering a custom, efficient serializer with Kryo.

### State Primitives and Access Patterns

Using the right state primitive for the job can dramatically improve performance, especially with RocksDB.

- **`ValueState<MyComplexObject>`**: Stores a single, potentially large object.

  - **Problem**: If you only need to change one field in `MyComplexObject`, you still have to read the _entire_ object from RocksDB, deserialize it, change the field, serialize the _entire_ object back, and write it to RocksDB. This is very inefficient.

- **`MapState<String, MyFieldValue>`**: Stores key-value pairs.
  - **Solution**: Instead of one large object, model its fields as entries in a `MapState`. Now, to update a single field, you only read, deserialize, serialize, and write that one small entry. This results in much less I/O and CPU overhead.

**Example: Migrating from `ValueState` to `MapState`**

**Inefficient Approach:**

```kotlin
// Data class representing a user's profile
data class UserProfile(var name: String, var lastLogin: Long, var preferences: Map<String, String>)

// In a KeyedProcessFunction...
private lateinit var profileState: ValueState<UserProfile>

override fun processElement(...) {
    val currentProfile = profileState.value()
    // To update just the login time, we must read and write the whole object
    currentProfile.lastLogin = System.currentTimeMillis()
    profileState.update(currentProfile)
}
```

**Efficient Approach:**

```kotlin
// In a KeyedProcessFunction...
private lateinit var profileMapState: MapState<String, Any>

override fun processElement(...) {
    // To update just the login time, we access only that specific key
    profileMapState.put("lastLogin", System.currentTimeMillis())
}
```

## Preventing Leaking State

In long-running applications, a common problem is state that grows indefinitely. If your keys have high cardinality (e.g., a new key for every event), your application's state size can explode, eventually leading to performance degradation or failure. Flink provides two primary mechanisms to manage state size automatically.

### Using State Time-To-Live (TTL)

State TTL is the easiest and most common way to automatically clean up state that is no longer needed. You configure it on the `StateDescriptor`.

**Function Signature (for the builder):**

```kotlin
class StateTtlConfig {
    // Main builder method
    static fun newBuilder(ttl: Time): Builder

    // Configuration options on the builder
    fun setUpdateType(updateType: UpdateType): Builder
    fun setStateVisibility(stateVisibility: StateVisibility): Builder
    // ... and more for cleanup strategies
}
```

**Kotlin Code Snippet with Detailed Configuration:**

```kotlin
import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.common.time.Time

fun getDescriptorWithTtl(): ValueStateDescriptor<String> {
    val ttlConfig = StateTtlConfig
        // State will be eligible for cleanup 10 minutes after its last modification.
        .newBuilder(Time.minutes(10))
        // Reset the TTL timer on every read and write access.
        // Use OnCreateAndWrite to only reset on writes.
        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
        // Do not return state to the user if it has already expired but not yet been cleaned up.
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        // Configure cleanup to happen during checkpointing.
        .cleanupInbackground()
        .build()

    val descriptor = ValueStateDescriptor("myStateWithTtl", String::class.java)
    descriptor.enableTimeToLive(ttlConfig)
    return descriptor
}
```

### Programmatic Cleanup with Process Timers

For more complex cleanup logic that isn't based solely on time-since-last-access, you can use **timers** within a `KeyedProcessFunction`. A timer is a one-shot trigger that fires at a specific time, invoking the `onTimer()` callback.

This pattern is very powerful: when an event for a key arrives, you can register a timer to fire in the future. If another event for the same key arrives before the timer fires, you can delete the old timer and register a new one. If the timer does fire, it means you haven't seen activity for that key in a while, and you can safely clear its state.

**Kotlin Code Snippet: Using a Timer to Clear State**

```kotlin
class StateClearingFunction(private val inactivityThresholdMs: Long) : KeyedProcessFunction<String, String, Unit>() {

    private lateinit var lastValueState: ValueState<String>

    override fun open(parameters: Configuration) {
        lastValueState = runtimeContext.getState(ValueStateDescriptor("lastValue", String::class.java))
    }

    override fun processElement(value: String, ctx: Context, out: Collector<Unit>) {
        // Update the state
        lastValueState.update(value)

        // Register a timer to fire in the future. This will also override any previous timer.
        val cleanupTime = ctx.timestamp() + inactivityThresholdMs
        ctx.timerService().registerEventTimeTimer(cleanupTime)
    }

    override fun onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector<Unit>) {
        // This method is called when the timer fires.
        // We clear the state for the current key.
        println("Timer fired for key ${ctx.getCurrentKey()} at $timestamp. Clearing state.")
        lastValueState.clear()
    }
}
```
