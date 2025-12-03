# Evolving Stateful Flink Applications

This document provides a comprehensive guide on how to safely evolve and maintain stateful Apache Flink applications over time. Managing application upgrades without losing critical state is a fundamental challenge in stream processing. We will cover savepoint-compatible (and incompatible) strategies for adding and removing operators, and for modifying state schemas.

## Table of Contents

- [The Golden Rule: Assign Unique Identifiers (UIDs)](#the-golden-rule-assign-unique-identifiers-uids)
- [Updating an Application without Modifying Existing State](#updating-an-application-without-modifying-existing-state)
  - [Adding a Stateless Operator](#adding-a-stateless-operator)
  - [Adding a New Stateful Operator](#adding-a-new-stateful-operator)
- [Removing State from an Application](#removing-state-from-an-application)
  - [The Default Safety Check](#the-default-safety-check)
  - [Allowing Non-Restored State](#allowing-non-restored-state)
- [Modifying the State of an Operator](#modifying-the-state-of-an-operator)
  - [Changing State Data Types (Schema Evolution)](#changing-state-data-types-schema-evolution)
  - [Changing State Primitives](#changing-state-primitives)

## The Golden Rule: Assign Unique Identifiers (UIDs)

Before attempting any of the following patterns, you must understand the most important prerequisite for evolving a stateful Flink application: **you must assign a stable, unique identifier (UID) to every operator in your job graph.**

When you restore from a savepoint, Flink uses these UIDs to map the state stored in the savepoint back to the operators in your new application. Without UIDs, Flink generates them automatically, but these generated IDs can change with any minor modification to your job's topology, making it impossible to restore state reliably.

**Function Signature:**

```kotlin
fun <T> DataStream<T>.uid(uid: String): DataStream<T>
```

**Best Practice:** Make it a strict rule to add a `.uid()` call after every single operator in your topology.

```kotlin
val stream: DataStream<Event> = env
    .fromSource(...)
    .uid("events-kafka-source") // UID for the source
    .keyBy { it.userId }
    .process(...)
    .uid("user-session-processor") // UID for the stateful operator
    .sinkTo(...)
    .uid("results-database-sink") // UID for the sink
```

## Updating an Application without Modifying Existing State

This is the most common and straightforward evolution scenario. As long as you maintain the UIDs of your existing operators, you can freely add new operators to your topology.

### Adding a Stateless Operator

You can add new stateless operators (like `map`, `filter`, or a `FlatMapFunction` without state) anywhere in the job graph. When you restore from a savepoint, Flink will map the state to the existing stateful operators using their UIDs and simply run the new operator as part of the data flow.

**Example: Adding a `filter` operator.**

**Before:** A simple pipeline that processes events.

```kotlin
// Version 1
val stream = env.fromSource(...).uid("source")
    .keyBy { it.key }
    .process(MyStatefulFunction()).uid("stateful-processor")
    .sinkTo(...).uid("sink")
```

**After:** We add a stateless filter before the stateful processor.

```kotlin
// Version 2
val stream = env.fromSource(...).uid("source")
    .filter { it.value > 0 }.uid("positive-value-filter") // New stateless operator with a UID
    .keyBy { it.key }
    .process(MyStatefulFunction()).uid("stateful-processor") // UID is unchanged
    .sinkTo(...).uid("sink")
```

**Result:** This is a **fully savepoint-compatible** change. The state for `"stateful-processor"` will be restored correctly.

### Adding a New Stateful Operator

You can also add a new stateful operator to the topology. When you restore from a savepoint, Flink will restore the state for all existing operators it can find UIDs for. For the new stateful operator, Flink will see that there is no previous state in the savepoint and will simply initialize it with empty state.

**Example: Adding a new stateful counter.**

**Before:** The same initial pipeline.

```kotlin
// Version 1
val stream = env.fromSource(...).uid("source")
    .keyBy { it.key }
    .process(MyStatefulFunction()).uid("stateful-processor")
    .sinkTo(...).uid("sink")
```

**After:** We add a new stateful function to count events after the first processor.

```kotlin
// Version 2
val stream = env.fromSource(...).uid("source")
    .keyBy { it.key }
    .process(MyStatefulFunction()).uid("stateful-processor") // UID is unchanged
    .process(new EventCounterFunction()).uid("event-counter") // New stateful operator
    .sinkTo(...).uid("sink")
```

**Result:** This is a **fully savepoint-compatible** change. `"stateful-processor"` will have its state restored, and `"event-counter"` will start with fresh, empty state.

## Removing State from an Application

Removing a stateful operator is more complex because it means you are intentionally discarding its state. Flink has a safety mechanism to prevent accidental state loss.

### The Default Safety Check

By default, when you restore a job from a savepoint, Flink checks that every piece of state in the savepoint can be mapped to a compatible operator in the new job graph. If you remove a stateful operator, its UID will be missing from the new topology, and the restore operation will **fail with an exception**.

**Example: Removing `"stateful-processor"`.**

**Before:**

```kotlin
// Version 1
val stream = env.fromSource(...).uid("source")
    .keyBy { it.key }
    .process(MyStatefulFunction()).uid("stateful-processor")
    .sinkTo(...).uid("sink")
```

**After:**

```kotlin
// Version 2 - "stateful-processor" is removed
val stream = env.fromSource(...).uid("source")
    .sinkTo(...).uid("sink")
```

**Result:** Running `flink run -s /path/to/savepoint ...` will **fail**. Flink will report that it found state for an operator with UID `"stateful-processor"` in the savepoint but could not find a matching operator in the new job.

### Allowing Non-Restored State

To explicitly discard the state of a removed operator, you can use the `--allow-non-restored-state` flag when running your job. This tells the Flink JobManager to ignore any state in the savepoint that it cannot map to the new job graph.

**Command-line syntax:**

```bash
./bin/flink run -s /path/to/savepoint --allow-non-restored-state your-job.jar
```

**Best Practice:** This is a powerful but destructive operation. Use this flag with caution and only when you are certain that discarding the state of the removed operator is the intended outcome.

## Modifying the State of an Operator

Modifying the data structures or data types within an existing stateful operator is the most complex evolution scenario.

### Changing State Data Types (Schema Evolution)

Simply changing a field in a Kotlin data class used in your state (e.g., adding a field, changing a type from `Int` to `Long`) is often an **incompatible change**. Flink's default serializer (Kryo) is not designed for schema evolution and will likely fail to deserialize the old data, causing the job restoration to fail.

**The Solution: Use a Serialization Framework with Schema Evolution Support**

The recommended way to handle schema evolution is to use a format like **Apache Avro**. Avro serializes objects along with their schema, and it has well-defined rules for evolving schemas in a compatible way (e.g., adding a new field with a default value).

**Example: Evolving an Avro-based state.**

1.  **Add the `flink-avro` dependency** to your project.
2.  **Define your state type using Avro.** Flink can generate classes from Avro schemas, or you can use reflection-based serializers.
3.  **Configure the state descriptor to use the Avro serializer.**

**Kotlin Code Snippet:**

```kotlin
// Assuming 'UserProfileV1' is a class generated from an Avro schema
val descriptor = ValueStateDescriptor(
    "user-profile",
    TypeInformation.of(UserProfileV1::class.java) // Flink will use the Avro serializer
)
```

**Evolution Path:**

1.  **Version 1:** `UserProfileV1` has fields `name` and `email`.
2.  **Take a savepoint.**
3.  **Version 2:** Evolve the Avro schema to `UserProfileV2` by adding a new optional field `lastLogin` with a default value of `null`. This is a backward-compatible change according to Avro's rules.
4.  **Restore from the savepoint** using the new job code that uses `UserProfileV2`.

**Result:** This is a **savepoint-compatible** change. Flink, using Avro, will successfully deserialize the V1 data into V2 objects, populating the new `lastLogin` field with its default value.

### Changing State Primitives

Changing the fundamental type of state primitive for an operator is an **incompatible change** that cannot be handled automatically.

For example, you cannot restore a savepoint if you change your code from:

```kotlin
// Version 1
val descriptor = ValueStateDescriptor("user-data", UserProfile::class.java)
val userState: ValueState<UserProfile> = runtimeContext.getState(descriptor)
```

to:

```kotlin
// Version 2
val descriptor = MapStateDescriptor("user-data", String::class.java, String::class.java)
val userState: MapState<String, String> = runtimeContext.getMapState(descriptor)
```

Even though the state name `"user-data"` is the same, Flink stores metadata about the state primitive type (`ValueState` vs. `MapState`) in the savepoint. It will refuse to restore the state because the types do not match.

**Solution: Manual State Migration**

To perform such a change, you must resort to a manual state migration process, which is outside the scope of a simple savepoint restore. The general steps are:

1.  In your old application (Version 1), add a new sink that reads the state and writes it to a durable external storage (like a Kafka topic or a database).
2.  Run the old application to export all its state.
3.  Start your new application (Version 2) with an empty state.
4.  Use a `SourceFunction` or a connected stream to read the exported state from the external storage and use it to populate the new `MapState` in your application. This is often called "bootstrapping" the state.
