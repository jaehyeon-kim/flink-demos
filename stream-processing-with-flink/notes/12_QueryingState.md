# Querying Flink State: CQRS and Remote State Backends

With the deprecation and removal of Flink's original Queryable State feature, the community has embraced more robust, scalable, and operationally sound architectural patterns. This guide provides a deep dive into the two primary, modern solutions for making Flink state accessible to external applications: the **CQRS Pattern via a Materialized View** and the **Remote State Backend architecture with Apache Fluss**.

## Table of Contents

- [Pattern 1: The CQRS Pattern via a Materialized View](#pattern-1-the-cqrs-pattern-via-a-materialized-view)
  - [Concept and Recommendation](#concept-and-recommendation)
  - [Architecture and Trade-offs](#architecture-and-trade-offs)
  - [Implementation: The Idempotent Sink](#implementation-the-idempotent-sink)
- [Pattern 2: The Remote State Backend Pattern (Apache Fluss)](#pattern-2-the-remote-state-backend-pattern-apache-fluss)
  - [Concept and Status](#concept-and-status)
  - [Architecture and Trade-offs](#architecture-and-trade-offs-1)
- [Architectural Comparison and Recommendations](#architectural-comparison-and-recommendations)

## Pattern 1: The CQRS Pattern via a Materialized View

This is the most common, battle-tested, and widely recommended pattern for querying Flink state in production systems today. It leverages the formal architectural pattern of **Command Query Responsibility Segregation (CQRS)**.

### Concept and Recommendation

In a CQRS architecture, the system is split into two distinct sides:

- **The Command Side**: Handles state changes. In our case, the Flink application is the command side. It processes the stream of events and issues "commands" (writes/updates) to change the state.
- **The Query Side**: Handles reads. A separate, dedicated database (e.g., Redis, PostgreSQL, Elasticsearch) serves as the query side, optimized for read requests from external applications.

The Flink job computes the results and uses a sink to create a **Materialized View** of its state in the external query database.

**This pattern is the current best practice for the vast majority of Flink use cases.** It provides a clean separation of concerns that is scalable, flexible, and operationally robust.

### Architecture and Trade-offs

- **Architecture**:
  `Data Stream -> Flink Job (Command Side) -> Idempotent Sink -> External DB (Query Side) <- External Applications`
- **Trade-offs**:
  - **(+) Pro (Decoupling & Scalability)**: The processing (Flink) and query-serving (DB) layers are completely independent. You can scale them separately, and a high query load will not impact your Flink job's stability.
  - **(+) Pro (Flexibility)**: You can choose the best database for your query needs (key-value, SQL, search, etc.).
  - **(-) Con (Data Duplication)**: State is stored in two places: inside Flink's state backend (e.g., RocksDB) and in the external database.
  - **(-) Con (Eventual Consistency)**: There is a minimal latency between Flink updating its internal state and the materialized view reflecting that change.

### Implementation: The Idempotent Sink

The key to a successful CQRS implementation is an **idempotent sink**, which ensures that duplicate writes during a Flink recovery do not corrupt the state in the database. This is typically achieved with an "upsert" operation.

**Kotlin Code Snippet: Idempotent JDBC Sink for PostgreSQL**

```kotlin
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement

data class UserProfile(val userId: Long, val name: String, val lastLogin: Long)

class JdbcUpsertSink(private val jdbcUrl: String) : RichSinkFunction<UserProfile>() {
    private lateinit var connection: Connection
    private lateinit var upsertStatement: PreparedStatement

    override fun open(parameters: Configuration) {
        connection = DriverManager.getConnection(jdbcUrl)

        // The UPSERT statement for PostgreSQL is the key to idempotency.
        val upsertSql = """
            INSERT INTO user_profiles (user_id, name, last_login) VALUES (?, ?, ?)
            ON CONFLICT (user_id) DO UPDATE SET name = EXCLUDED.name, last_login = EXCLUDED.last_login;
        """
        upsertStatement = connection.prepareStatement(upsertSql)
    }

    override fun invoke(profile: UserProfile, context: Context) {
        upsertStatement.setLong(1, profile.userId)
        upsertStatement.setString(2, profile.name)
        upsertStatement.setLong(3, profile.lastLogin)
        upsertStatement.executeUpdate()
    }

    override fun close() {
        if (::upsertStatement.isInitialized) upsertStatement.close()
        if (::connection.isInitialized) connection.close()
    }
}
```

## Pattern 2: The Remote State Backend Pattern (Apache Fluss)

This is an emerging, high-performance pattern that directly addresses the trade-offs of the CQRS approach. It is built on the principle of **Separation of Storage and Compute**.

### Concept and Status

In this pattern, Flink's state is not stored on the local disk of the TaskManagers. Instead, Flink is configured with a **Remote State Backend** that writes state over the network to a specialized, distributed storage system. External applications can then query this remote system directly.

**Apache Fluss** is an official Apache Incubator project that provides an implementation of this architecture.

While this pattern is the future direction for Flink state management, it's important to note that **Apache Fluss is currently an incubating project**. This means it is less mature and battle-tested than the standard databases used in the CQRS pattern.

### Architecture and Trade-offs

- **Architecture**:
  `Flink Job <-> Remote State Backend (e.g., Apache Fluss) <- External Applications`
- **Trade-offs**:
  - **(+) Pro (Single Source of Truth)**: Eliminates data duplication and eventual consistency issues. The queried state is the _actual_ state used by Flink.
  - **(+) Pro (Faster Scaling & Recovery)**: Flink TaskManagers become stateless compute nodes, allowing for much faster application scaling and recovery.
  - **(-) Con (Maturity)**: The technology is still under active development and not as widely deployed as traditional databases.
  - **(-) Con (Operational Complexity)**: It requires deploying and managing a new, specialized distributed system (the Fluss cluster) in addition to your Flink cluster.

## Architectural Comparison and Recommendations

| Feature                | CQRS Pattern (via Materialized View)                    | Remote State Backend (via Apache Fluss)                     |
| :--------------------- | :------------------------------------------------------ | :---------------------------------------------------------- |
| **Data Model**         | State is duplicated (inside Flink + external copy)      | **Single source of truth**                                  |
| **Consistency**        | Eventually Consistent                                   | Strongly Consistent (or Read-Your-Writes)                   |
| **Maturity**           | **Highly Mature.** Uses standard, production-ready DBs. | **Incubating.** Still under active development.             |
| **Flexibility**        | High. Can choose any DB that fits the query pattern.    | Lower. Tied to the query capabilities of the state backend. |
| **Operational Effort** | Deploy & manage Flink + a standard database.            | Deploy & manage Flink + a Fluss cluster.                    |

### Recommendations

- **For the vast majority of production use cases today, the CQRS Pattern is the recommended approach.** It is robust, flexible, operationally understood, and leverages the mature ecosystem of existing databases.

- **The Remote State Backend pattern is the future direction for Flink.** Consider it for advanced, high-performance use cases that cannot tolerate the eventual consistency or data duplication of the CQRS pattern and where the operational overhead of managing a cutting-edge system like Apache Fluss is acceptable.
