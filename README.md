<a name="storm-sideline-spout"></a>
# Storm Sidelining Kafka Spout

## Purpose of this project
The purpose of this project is to provide a [Kafka (0.10.0.x)](https://kafka.apache.org/) based spout for [Apache Storm (1.0.x)](https://storm.apache.org/) that provides the ability
to dynamically "*sideline*" or skip specific messages to be replayed at a later time based on a set of filter criteria.

Under normal circumstances this spout works much like your typical [Kafka-Spout](https://github.com/nathanmarz/storm-contrib/tree/master/storm-kafka) and
aims to be a drop in replacement for it.  This implementation differs in that it exposes trigger and 
filter semantics when you build your topology which allow for specific messages to be skipped, and then
replayed at a later point in time.  And it does dynamically without requiring you to re-deploy your topology as the 
criteria changes!

### Example use cases
Wow! That sounds interesting...but when would I actually need this?

#### Multi-tenant processing
When consuming a multi-tenant commit log you may want to postpone processing for one or more tenants. Imagine 
that a subset of your tenants database infrastructure requires downtime for maintenance.  Using the Kafka-Spout
implementation you really only have two options to deal with this situation.  

You could stop your entire topology for all tenants while the maintenance is performed for the small subset of tenants.  

Or you could filter these tenants out from being processed by your topology, then after the maintenance is complete start
a separate topology/Kafka-Spout instance that somehow knows where to start and stop consuming, and by way of down-stream filter bolts
re-process only the events for the tenants that were previously filtered.  

Unfortunately both of these solutions are either not acceptable, complicated, error prone, not to mention down right painful.

#### Some other use case here
Surely we can come up with another use case.
 
## How does it work?
This spout implementation exposes 2 interfaces for controlling **WHEN** and **WHAT** messages from Kafka get
skipped and marked for processing at a later point in time.

The **Trigger Interface** allows you to define **WHEN** the spout will start marking messages for delayed processing,
and **WHEN** the spout will start processing messages that it previously skipped.

The **Filter Interface** allows you to define **WHAT** messages the spout will mark for delayed processing.

The spout implementation handles the rest for you!  It tracks your filter criteria as well as offsets within
Kafka topics to know where it started and stopped filtering.  It then uses this metadata to replay only those 
messages which got filtered.

## No really... How does it work?

Lets define our major components of the Spout and give a brief explanation of what their role is.  Then we'll
build up how they all work together.

### Introduction to the internal components

[SidelineSpout](src/main/java/com/salesforce/storm/spout/sideline/SidelineSpout.java) - Implements Storm's spout interface.  Everything starts and stops here.  

[SidelineConsumer](src/main/java/com/salesforce/storm/spout/sideline/kafka/SidelineConsumer.java) - This is our high-level Kafka consumer built ontop of [KafkaConsumer]() that handles consuming from
Kafka topics as well as maintaining consumer state information.  It wraps KafkaConsumer
giving it semantics that play nicely with Storm.  KafkaConsumer assumes messages from a given partition are always
consumed in order and processed in order.  As we know Storm provides no guarantee that as those messages get converted to tuples
and emitted into your topology, they may get processed in no particular order.  Because of this, tracking which offsets
within your Kafka topic have or have not been processed is not entirely trivial.  

[VirtualSidelineSpout](src/main/java/com/salesforce/storm/spout/sideline/kafka/VirtualSidelineSpout.java) - Within a SidelineSpout instance, you will have one or more VirtualSidelineSpout instances.
These wrap SidelineConsumer instances in order to consume messages from Kafka, and layers on functionality to determine
which should be emitted into the topology, tracking which tuples have been ack'd, and which have failed.  

[SpoutRunner](src/main/java/com/salesforce/storm/spout/sideline/coordinator/SpoutRunner.java) - VirtualSidelineSpout instances are always run within their own processing Thread.  SpoutRunner is
the wrapper around VirtualSidelineSpout that manages the Thead it runs within.

[SpoutMonitor](src/main/java/com/salesforce/storm/spout/sideline/coordinator/SpoutMonitor.java) - This monitors new SidelineRequests via your implemented [Triggers]().  

When a [StartSidelineRequest] is triggered, it will start applying filter criteria to tuples being emitted
from the spout.  Additionally it will track offsets within your topic of where it started filtering.

When a [StopSidelineRequest] is triggered, it will stop applying the filter criteria to tuples being emitted from the
spout. Additionally it will determine which offsets the filtering criteria were applied at and use this information
to start a new VirtualSidelineSpout instance.  This VirtualSidelineSpout instance will be configured to start
consuming at these offsets, and stop consuming once it reaches the offsets in which the filtering criteria was removed.
The VirtualSidelineSpout instance will only emit messages from Kafka that were filtered during this period.  Once
the VirtualSidelineSpout has processed all of the offsets within the topic, SpoutMonitor will shut it down.

[SpoutCoordinator](src/main/java/com/salesforce/storm/spout/sideline/SpoutCoordinator.java) - This bridges the gap between our SidelineSpout and its internal VirtualSidelineSpouts.  

As nextTuple() is called on SidelineSpout, it asks SpoutCoordinator for the next kafka message that should be emitted. 
The SpoutCoordinator gets the next message from one of its many VirtualSidelineSpout instances.

As fail() is called on SidelineSpout, the SpoutCoordinator determines which VirtualSidelineSpout instance
the failed tuple originated from and passes it to the correct instance's fail() method.

As ack() is called on SidelineSpout, the SpoutCoordinator determines which VirtualSidelineSpout instance
the acked tuple originated from and passes it to the correct instance's ack() method.

[PersistenceManager](src/main/java/com/salesforce/storm/spout/sideline/persistence/PersistenceManager.java) - This provides a persistence layer for storing SidelineSpout's metadata.  It stores things
such as consumer state/offsets for consuming, as well as metadata about SidelineRequests.
 
### So....How does it work?

[Insert info graphic here]

#### First Deploy
When the SidelineSpout is first deployed to your Topology it starts the SpoutMonitor.  The SpoutMonitor then
creates the *main* VirtualSidelineSpout instance.  This *main* VirtualSidelineSpout instance is always running within
the spout, and its job is to consume from your configured Kafka topic.  As it consumes messages from Kafka, it deserializes
them using your [Deserializer]() implementation.  It then runs it thru a [FilterChain](), which is a collection of
[FilterChainSteps]().  These filters determine what messages should be *sidelined* and which should be emitted out.
When no [StartSidelineRequests]() are active, this FilterChain is empty, and all messages consumed from
Kafka will be converted to Tuples and emitted to your topology.

[Insert info graphic here]

#### Start Sideline Request
Your implemented [Trigger]() will notify the [SpoutMonitor]() that a new Start Sidelining Request has occurred.  The SpoutMonitor
will record the *main* VirtualSidelineSpout's current offsets within the topic and record them with request via
your configured [PersistenceManager]() implementation.  The SpoutMonitor will then take the FilterChainStep associated with the request and attach it to the *main* VirtualSidelineSpout's FilterChain.
At this point any new messages from Kafka that the *main* VirtualSidelineSpout consumes will pass through this modified FilterChain.
ANY messages that match the criteria will be skipped, while messages that do not match the criteria will pass through and
get processed.

[Insert info graphic here]

#### Stop Sideline Request
Your implemented [Trigger]() will notify the [SpoutMonitor]() that it would like to stop a Sideline Request.  The SpoutMonitor
will first determine which [FilterChainStep]() was associated with the request and remove it from the *main* VirtualSidelineSpout instance's
[FilterChain]().  It will also record the *main* VirtualSidelineSpout's current offsets within the topic and record them via your
configured [PersistenceManager]() implementation.  At this point messages consumed from the Kafka topic will no longer be filtered.
The SpoutMonitor will create a new instance of VirtualSidelineSpout configured to start consuming from the offsets
recorded during the Start Sideline Request when filtering first began, and stop consuming once it's reached the offsets where
filtering was stopped.  Additionally it will take the [FilterChainStep]() that was used to filter the *main* VirtualSidelineSpout
instance for the request, and negate its criteria.  This will in effect filter all the messages that the *main* VirtualSidelineSpout 
processed, only allowing those that got filtered to pass and get emitted for processing by the topology.

Once the VirtualSidelineSpout has completed consuming the skipped offsets, it will automatically shut down.

[Insert info graphic here]

#### What happens if I stop and redeploy the topology?
The SidelineSpout has several moving pieces, all of which will properly handle resuming in the state that they were
when the topology was halted.  The *main* VirtualSidelineSpout will continue consuming from the last acked offsets within your topic.
Metadata about active StartSidelineRequests are retrieved via [PersistenceManager]() and resumed on start, properly filtering
messages from being emitted into the topology.  Metadata about active StopSidelineRequests are retrieved via [PersistenceManager](), and VirtualSidelineSpout instances are
started and resume consuming messages at the last previously acked offsets.

# Getting started
## Dependencies
Using the default straight-out-of-the-box configuration, this spout has the following dependencies:
- [Apache Storm 1.0.x](https://storm.apache.org/) - This one should be self explanatory.
- [Apache Kafka 0.10.0.x](https://kafka.apache.org/) - The underlying kafka consumer is based on this version of the Kafka-Client library.
- [Zookeeper](https://zookeeper.apache.org/) - Metadata the spout tracks has to be persisted somewhere, by default we use Zookeeper.  This is not
a hard dependency as you can write your own [PersistenceManager](src/main/java/com/salesforce/storm/spout/sideline/persistence/PersistenceManager.java) implementation to store this metadata
any where you would like.  Mysql? Redis? Sure!  Contribute an adapter to the project!

## Configuration

## Required Interface Implementations
### [Deserializer](src/main/java/com/salesforce/storm/spout/sideline/kafka/deserializer/Deserializer.java)
The Deserializer interface dictates how the kafka key and messages consumed from Kafka as byte[] gets transformed into a storm tuple. It also 
controls the naming of your output field(s).

```java
    /**
     * This is the method your implementation would need define.
     * A null return value from here will result in this message being ignored.
     *
     * @param topic - represents what topic this message came from.
     * @param partition - represents what partition this message came from.
     * @param offset - represents what offset this message came from.
     * @param key - byte array representing the key.
     * @param value - byte array representing the value.
     * @return Values that should be emitted by the spout to the topology.
     */
    Values deserialize(final String topic, final int partition, final long offset, final byte[] key, final byte[] value);

    /**
     * Declares the output fields for the deserializer.
     * @return An instance of the fields
     */
    Fields getOutputFields();
```

#### [AbstractScheme](src/main/java/com/salesforce/storm/spout/sideline/kafka/deserializer/compat/AbstractScheme.java)
For compatibility to Storm-Kafka's Scheme interface, you can instead extend [AbstractScheme](src/main/java/com/salesforce/storm/spout/sideline/kafka/deserializer/compat/AbstractScheme.java)
and use an existing implementation.

### [StartingTrigger](src/main/java/com/salesforce/storm/spout/sideline/trigger/StartingTrigger.java)
The StartingTrigger interface dictates how your running spout instance gets notified of new requests to filter and sideline
messages being consumed from Kafka.

```java
void setSidelineSpout(SpoutTriggerProxy spout);
```

### [StoppingTrigger](src/main/java/com/salesforce/storm/spout/sideline/trigger/StoppingTrigger.java)
The StoppingTrigger interface dictates how your running spout instance gets notified of new requests to remove a previously
started filter and start reprocessing any messages that were previously skipped.

```java
void setSidelineSpout(SpoutTriggerProxy spout);
```

### [FilterChainStep](src/main/java/com/salesforce/storm/spout/sideline/filter/FilterChainStep.java)
The FilterChainStep interface dictates how you want to filter messages being consumed from kafka.

```java
    /**
     * Inputs an object, performs some business logic on it and then returns the result.
     *
     * @param message The filter to be processed by this step of the chain
     * @return The resulting filter after being processed
     */
    boolean filter(KafkaMessage message);
```

## Optional Interfaces for Overachievers
For most use cases the above interfaces are all that are required to get going.  But..sometimes your use case requires
you to do do something just a little special of different.  If that's the case with you, we provide the following 
configurable interfaces to hopefully ease the pain.

### [PersistenceManager](src/main/java/com/salesforce/storm/spout/sideline/persistence/PersistenceManager.java)
This interface dictates how and where metadata gets stored such that it lives between topology deploys.
In an attempt to decouple this data storage layer from the spout, we have this interface.  Currently we have
one implementation backed by Zookeeper.

###### Configuration
Config Key   | Type | Description | Default Value |
------------ | ---- | ----------- | --------------
sideline_spout.persistence_manager.class | String | Defines which PersistenceManager implementation to use.    Should be a full classpath to a class that implements the PersistenceManager interface. | *null* 


#### Provided Implementations
##### [ZookeeperPersistenceManager](src/main/java/com/salesforce/storm/spout/sideline/persistence/ZookeeperPersistenceManager.java)
This is our default implementation, it uses a Zookeeper cluster to persist the required metadata.

###### Configuration
Config Key   | Type | Description | Default Value |
------------ | ---- | ----------- | --------------
sideline_spout.persistence.zk_servers | List\<String\> | Holds a list of Zookeeper server Hostnames + Ports in the following format: ["zkhost1:2181", "zkhost2:2181", ...] | *null* 
sideline_spout.persistence.zk_root | String | Defines the root path to persist state under.  Example: "/sideline-consumer-state" | *null*

##### [InMemoryPersistenceManager](src/main/java/com/salesforce/storm/spout/sideline/persistence/InMemoryPersistenceManager.java)
This implementation only stores metadata within memory.  This is useful for tests, but has no real world use case as all state will be lost between topology deploys.

### [RetryManager](src/main/java/com/salesforce/storm/spout/sideline/kafka/retryManagers/RetryManager.java)
Interface for handling failed tuples.  By creating an implementation of this interface you can control how the spout deals with tuples that have failed within the topology. Currently we have
three implementations bundled with the spout which should cover most standard use cases.

###### Configuration
Config Key   | Type | Description | Default Value |
------------ | ---- | ----------- | --------------
sideline_spout.retry_manager.class | String | Defines which RetryManager implementation to use.  Should be a full classpath to a class that implements the RetryManager interface. |"com.salesforce.storm.spout.sideline.kafka.retryManagers.DefaultRetryManager"

#### Provided Implementations
##### [DefaultRetryManager](src/main/java/com/salesforce/storm/spout/sideline/kafka/retryManagers/DefaultRetryManager.java)
This is our default implementation for the spout.  It attempts retries of failed tuples a maximum of MAX_RETRIES times.
After a tuple fails more than that, it will be "acked" or marked as completed and never tried again.
Each retry is attempted using an exponential back-off time period.  The first retry will be attempted within MIN_RETRY_TIME_MS milliseconds.  Each attempt
after that will be retried at (FAIL_COUNT * MIN_RETRY_TIME_MS) milliseconds.

###### Configuration
Config Key   | Type | Description | Default Value |
------------ | ---- | ----------- | --------------
sideline_spout.failed_msg_retry_manager.max_retries | int | Defines how many times a failed message will be replayed before just being acked.  A value of 0 means tuples will never be retried. A negative value means tuples will be retried forever. | 25
sideline_spout.failed_msg_retry_manager.min_retry_time_ms | long | Defines how long to wait before retry attempts are made on failed tuples, in milliseconds. Each retry attempt will wait for (number_of_times_message_has_failed * min_retry_time_ms).  Example: If a tuple fails 5 times, and the min retry time is set to 1000, it will wait at least (5 * 1000) milliseconds before the next retry attempt. | 1000

##### [FailedTuplesFirstRetryManager](src/main/java/com/salesforce/storm/spout/sideline/kafka/retryManagers/FailedTuplesFirstRetryManager.java)
This implementation will always retry failed tuples at the earliest chance it can.  No back-off strategy, no maximum times a tuple can fail.

##### [NeverRetryManager](src/main/java/com/salesforce/storm/spout/sideline/kafka/retryManagers/NeverRetryManager.java)
This implementation will never retry failed messages.  One and done.

### [TupleBuffer](src/main/java/com/salesforce/storm/spout/sideline/tupleBuffer/TupleBuffer.java)
This interface defines an abstraction around essentially a concurrent queue.  Abstracting this instead of using directly a queue object allows us to do things like
implement a "fairness" algorithm on the poll() method for pulling off of the queue. Using a straight ConcurrentQueue would give us FIFO semantics 
but with an abstraction we could implement round robin across kafka consumers, or any scheduling algorithm that you'd like.

This is getting into the nitty-gritty internals of the spout here, you would need a pretty special use case to mess around
with this one.

###### Configuration
Config Key   | Type | Description | Default Value |
------------ | ---- | ----------- | --------------
sideline_spout.coordinator.tuple_buffer.class | String | Defines which TupleBuffer implementation to use. Should be a full classpath to a class that implements the TupleBuffer interface. | "com.salesforce.storm.spout.sideline.tupleBuffer.RoundRobinBuffer"

#### Provided Implementations
##### [RoundRobinBuffer](src/main/java/com/salesforce/storm/spout/sideline/tupleBuffer/RoundRobinBuffer.java)
This is our default implementation, which is essentially round-robin.  Each virtual spout has its own queue that gets added too.  A very chatty
virtual spout will not block/overrun less chatty ones.  {@link #poll()} will RR through all the available
queues to get the next msg.
 
Internally we make use of BlockingQueues so that we can put an upper bound on the queue size.
Once a queue is full, any producer attempting to put more messages onto the queue will block and wait
for available space in the queue.  This acts to throttle producers of messages.
Consumers from the queue on the other hand will never block attempting to read from a queue, even if its empty.
This means consuming from the queue will always be fast.
 
##### [FIFOBuffer](src/main/java/com/salesforce/storm/spout/sideline/tupleBuffer/FIFOBuffer.java)
FIFO implementation.  Has absolutely no "fairness" between VirtualSpouts or any kind of "scheduling."

### [MetricsRecorder](src/main/java/com/salesforce/storm/spout/sideline/metrics/MetricsRecorder.java)
This is still a work in progress.  More details to come.

#### Provided Implementations
##### [StormRecorder](src/main/java/com/salesforce/storm/spout/sideline/metrics/StormRecorder.java)
##### [LogRecorder](src/main/java/com/salesforce/storm/spout/sideline/metrics/LogRecorder.java)

# Metrics

# Releases & Changelog 


