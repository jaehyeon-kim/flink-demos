# Source, Sink V2, and Async I/O

This document provides a comprehensive and detailed guide to the Flink 1.20 Source API, Sink V2 API, and Asynchronous I/O (Async I/O). It includes core concepts, class and interface signatures, detailed code examples in Kotlin, and best practices to serve as a valuable learning resource.

## Table of Contents

- [Flink Source API](#flink-source-api)
  - [Core Concepts and Components](#core-concepts-and-components)
  - [Source API Best Practices](#source-api-best-practices)
- [Sink V2 API](#sink-v2-api)
  - [Core Concepts and Components](#core-concepts-and-components-1)
  - [Sink V2 API Best Practices](#sink-v2-api-best-practices)
- [Async I/O](#async-io)
  - [Core Concepts and Components](#core-concepts-and-components-2)
  - [Async I/O Best Practices](#async-io-best-practices)

## Flink Source API

The Data Source API provides a unified mechanism for ingesting data into Flink, supporting both bounded (batch) and unbounded (streaming) workloads. It's designed for high performance, scalability, and robust state management for fault tolerance.

### Core Concepts and Components

A Flink Source is built around three fundamental components that work together to read data from an external system.

1.  **Splits**: A `SourceSplit` represents a finite portion of the data to be processed. For a streaming source like a network socket, the concept of a "split" can be more abstract. A single socket connection is not naturally divisible, so we will model the entire connection as a single split. Therefore, this source will have a parallelism of 1.
2.  **SplitEnumerator**: This component is the central coordinator of the source. It runs as a single instance on the JobManager. For our Netcat source, its job is simple: create one split representing the network endpoint and assign it to the single `SourceReader` that connects.
3.  **SourceReader**: The `SourceReader` runs on a TaskManager. It requests a split from the `SplitEnumerator`, connects to the source system described by the split (in this case, opening a socket), reads the data, and emits the records into the Flink data stream.

These components are brought together by the `Source` class, which acts as the main entry point and a factory for creating the enumerator and readers.

### Source API Best Practices

- **Keep Splits Small but Not Tiny**: Splits should be large enough to minimize overhead but small enough to allow for good parallelization and work distribution.
- **Enumerator State should be Lean**: The `SplitEnumerator` runs on the JobManager, so its state should be as compact as possible. In our example, we only stored file paths, not the full `FileLineSplit` object.
- **Idempotent Readers**: Whenever possible, design readers to be idempotent so that reprocessing a split on recovery does not cause data duplication or corruption. Using offsets, as shown, is a key technique for this.
- **Handle Dynamic Split Discovery**: For unbounded sources, the `SplitEnumerator` must have a mechanism to discover new splits (e.g., using a background thread to watch a directory) and assign them to readers.

## Sink V2 API

The Sink V2 API is the standard way to write data from a Flink stream to an external system. It provides a clean, unified interface for both batch and streaming, with robust support for different delivery guarantees like exactly-once.

### Core Concepts and Components

The Sink V2 API is modular and can be composed of up to four parts, depending on the requirements of the external system.

1.  **Sink**: The main entry point and factory. Its primary job is to create a `SinkWriter` and, if needed, the committer components.

    - **`createWriter(InitContext)`**: Creates the `SinkWriter`.
    - **`createCommitter()`**: _(Optional)_ Creates a `Committer` for two-phase commit protocols.
    - **`createGlobalCommitter()`**: _(Optional)_ Creates a `GlobalCommitter` for a final, centralized commit step.

2.  **SinkWriter**: The workhorse of the sink. It runs in parallel on TaskManagers and is responsible for writing incoming records to the destination.

    - **`write(T element, Context context)`**: Called for each record. This is where the data is sent to the external system.
    - **`prepareCommit(boolean flush)`**: Called on checkpoint. The writer flushes any buffered data and returns a collection of "committables". A committable is a piece of metadata needed to finalize the write operation (e.g., a transaction ID or a temporary file path).

3.  **Committer**: _(Optional)_ Runs on TaskManagers and receives committables from the `SinkWriter` after a successful checkpoint. It is responsible for the first phase of the commit.

    - **`commit(Collection<CommT> committables)`**: Commits the transactions described by the committables.

4.  **GlobalCommitter**: _(Optional)_ A single, non-parallel instance running on the JobManager. It performs the final commit action for a checkpoint, which is necessary for systems that require a centralized finalization step.
    - **`commit(Collection<GlobalCommT> globalCommittables)`**: Performs the global commit.

### Sink V2 API Best Practices

- **Idempotent Writes vs. Two-Phase Commits**: If your storage system supports idempotent writes (e.g., key-value stores), prefer them. It's simpler and often more efficient than a two-phase commit. Use the `Committer` and `GlobalCommitter` only when you need to wrap writes in transactions.
- **Buffer Efficiently**: The `SinkWriter` should buffer records in memory and write them in batches to the external system to improve throughput and reduce I/O operations. The `prepareCommit` or `flush` method is the place to ensure all buffered data is sent.
- **Handle Failures**: The `commit` methods of the committers should be retryable. If a commit fails, Flink will restart the committer and ask it to commit the same committables again.

## Async I/O

Async I/O is a Flink feature designed to improve the throughput of data enrichment steps that involve external lookups (e.g., querying a database or calling a REST API). Instead of blocking on each request, it allows a single parallel task to handle many requests and responses concurrently.

### Core Concepts and Components

1.  **AsyncFunction**: The core of the API is the `AsyncFunction`, which you implement to perform the asynchronous lookup. A `RichAsyncFunction` is also available for access to the runtime context.

    - **`asyncInvoke(IN, ResultFuture<OUT>)`**: This method is called for each record. Inside, you trigger your asynchronous request (e.g., using a non-blocking I/O client). When the result is returned, you complete the `ResultFuture`.
    - **`timeout(IN, ResultFuture<OUT>)`**: This optional method is called if the request does not complete within the specified timeout. You should complete the `ResultFuture` with an exception here.

2.  **AsyncDataStream**: This is a static helper class used to apply an `AsyncFunction` to a `DataStream`. It provides two modes:
    - **`unorderedWait(...)`**: This mode offers the best performance. It emits the result of an async request as soon as it completes, meaning the order of the output stream may differ from the input stream.
    - **`orderedWait(...)`**: This mode preserves the order of the stream. Results are buffered and emitted only after all preceding results are available.

### Async I/O Best Practices

- **Use a Non-Blocking Client**: To get the full benefit of Async I/O, you must use a database driver or HTTP client that is truly asynchronous (non-blocking) and returns a future-like object (e.g., `CompletableFuture`). Using a blocking client in a separate thread pool does not achieve the same efficiency.
- **Tune Capacity**: The `capacity` parameter is critical. It controls how many async requests can be in-flight at once. Set this value carefully to maximize throughput without overwhelming the external database or service.
- **Choose `unorderedWait`**: Always prefer `unorderedWait` unless strict ordering is a business requirement. It delivers significantly better latency and throughput.
- **Implement Timeouts**: Always implement the `timeout` method to handle cases where the external service is slow or unresponsive. This prevents the Flink job from stalling.
