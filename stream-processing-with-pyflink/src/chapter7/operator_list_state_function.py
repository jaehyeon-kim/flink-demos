# Deprecation of ListCheckpointed interface #
# FLINK-6258 #
# The ListCheckpointed interface has been deprecated because it uses Java Serialization for checkpointing state which is problematic for savepoint compatibility. Use the CheckpointedFunction interface instead, which gives more control over state serialization.

# Operator State
# https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/datastream/fault-tolerance/state/

# Operator State (or non-keyed state) is state that is bound to one parallel operator instance. The Kafka Connector is a good motivating example for the use of Operator State in Flink. Each parallel instance of the Kafka consumer maintains a map of topic partitions and offsets as its Operator State.

# The Operator State interfaces support redistributing state among parallel operator instances when the parallelism is changed. There are different schemes for doing this redistribution.

# In a typical stateful Flink Application you don’t need operators state. It is mostly a special type of state that is used in source/sink implementations and scenarios where you don’t have a key by which state can be partitioned.

# Notes: Operator state is still not supported in Python DataStream API.
