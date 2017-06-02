# Storm Sidelining Kafka Spout

## Purpose of this project
The purpose of this project is to provide a [Kafka (0.10.0.x)](https://kafka.apache.org/) based spout for [Apache Storm (1.0.x)](https://storm.apache.org/) that provides the ability
to dynamically "*sideline*" or skip specific messages to be replayed at a later time based on a set of filter criteria.

Under normal circumstances this spout works much like your typical [Kafka-Spout](https://github.com/nathanmarz/storm-contrib/tree/master/storm-kafka) and
aims to be a drop in replacement for it.  This implementation differs in that it exposes trigger and 
filter semantics when you build your topology which allow for specific messages to be skipped, and then
replayed at a later point in time.  All this is done dynamically without requiring you to re-deploy your topology when filtering 
criteria changes!

## Table of Contents

  * [Storm Sidelining Kafka Spout](#storm-sidelining-kafka-spout)
    * [Purpose of this project](#purpose-of-this-project)
    * [Table of Contents](#table-of-contents)
      * [Example use case: Multi-tenant processing](#example-use-case-multi-tenant-processing)
    * [How does it work?](#how-does-it-work)
    * [How does it  <em>really</em> work?](#how-does-it--really-work)
      * [Primary Components](#primary-components)
      * [When the Topology Starts](#when-the-topology-starts)
      * [Starting Sideline Request](#starting-sideline-request)
      * [Stoping Sideline Request](#stoping-sideline-request)
      * [Stopping &amp; Redeploying the topology?](#stopping--redeploying-the-topology)
  * [Getting started](#getting-started)
    * [Dependencies](#dependencies)
    * [Configuration](#configuration)
      * [Required Configuration](#required-configuration)
    * [Required Interface Implementations](#required-interface-implementations)
      * [<a href="src/main/java/com/salesforce/storm/spout/sideline/kafka/deserializer/Deserializer.java">Deserializer</a>](#deserializer)
      * [<a href="src/main/java/com/salesforce/storm/spout/sideline/trigger/StartingTrigger.java">StartingTrigger</a>](#startingtrigger)
      * [<a href="src/main/java/com/salesforce/storm/spout/sideline/trigger/StoppingTrigger.java">StoppingTrigger</a>](#stoppingtrigger)
      * [<a href="src/main/java/com/salesforce/storm/spout/sideline/filter/FilterChainStep.java">FilterChainStep</a>](#filterchainstep)
    * [Example Trigger Implementation](#example-trigger-implementation)
    * [Optional Interfaces for Overachievers](#optional-interfaces-for-overachievers)
      * [<a href="src/main/java/com/salesforce/storm/spout/sideline/persistence/PersistenceAdapter.java">PersistenceAdapter</a>](#persistenceadapter)
            * [Configuration](#configuration-1)
      * [<a href="src/main/java/com/salesforce/storm/spout/sideline/kafka/retryManagers/RetryManager.java">RetryManager</a>](#retrymanager)
            * [Configuration](#configuration-2)
      * [<a href="src/main/java/com/salesforce/storm/spout/sideline/messageBuffer/MessageBuffer.java">MessageBuffer</a>](#messagebuffer)
            * [Configuration](#configuration-3)
    * [Optional Interfaces Implementations](#optional-interfaces-implementations)
      * [PersistenceAdapter Implementations](#persistenceadapter-implementations)
        * [<a href="src/main/java/com/salesforce/storm/spout/sideline/persistence/ZookeeperPersistenceAdapter.java">ZookeeperPersistenceAdapter</a>](#zookeeperpersistenceadapter)
            * [Configuration](#configuration-4)
        * [<a href="src/main/java/com/salesforce/storm/spout/sideline/persistence/InMemoryPersistenceAdapter.java">InMemoryPersistenceAdapter</a>](#inmemorypersistenceadapter)
      * [RetryManager Implementations](#retrymanager-implementations)
        * [<a href="src/main/java/com/salesforce/storm/spout/sideline/kafka/retryManagers/DefaultRetryManager.java">DefaultRetryManager</a>](#defaultretrymanager)
            * [Configuration](#configuration-5)
        * [<a href="src/main/java/com/salesforce/storm/spout/sideline/kafka/retryManagers/FailedTuplesFirstRetryManager.java">FailedTuplesFirstRetryManager</a>](#failedtuplesfirstretrymanager)
        * [<a href="src/main/java/com/salesforce/storm/spout/sideline/kafka/retryManagers/NeverRetryManager.java">NeverRetryManager</a>](#neverretrymanager)
      * [MessageBuffer Implementations](#messagebuffer-implementations)
        * [<a href="src/main/java/com/salesforce/storm/spout/sideline/messageBuffer/RoundRobinBuffer.java">RoundRobinBuffer</a>](#roundrobinbuffer)
        * [<a href="src/main/java/com/salesforce/storm/spout/sideline/messageBuffer/FIFOBuffer.java">FIFOBuffer</a>](#fifobuffer)
      * [MetricsRecorder Implementations](#metricsrecorder-implementations)
        * [<a href="src/main/java/com/salesforce/storm/spout/sideline/metrics/StormRecorder.java">StormRecorder</a>](#stormrecorder)
        * [<a href="src/main/java/com/salesforce/storm/spout/sideline/metrics/LogRecorder.java">LogRecorder</a>](#logrecorder)
    * [Metrics](#metrics)
  * [Interesting Ideas and Questions](#interesting-ideas-and-questions)
  * [Releases &amp; Changelog](#releases--changelog)

### Example use case: Multi-tenant processing
When consuming a multi-tenant commit log you may want to postpone processing for one or more tenants. Imagine 
that a subset of your tenants database infrastructure requires downtime for maintenance.  Using the Kafka-Spout
implementation you really only have two options to deal with this situation:

1. You could stop your entire topology for all tenants while the maintenance is performed for the small subset of tenants.  

2. You could filter these tenants out from being processed by your topology, then after the maintenance is complete start a separate topology/Kafka-Spout instance that somehow knows where to start and stop consuming, and by way ofown-stream filter bolts re-process only the events for the tenants that were previously filtered.  

Unfortunately both of these solutions are complicated, error prone and down right painful.
 
## How does it work?
The sideline spout is really a container of many virtual spouts, which handle processing messages from the firehose as well as from various sidelines once they are stopped.

This spout implementation exposes two interfaces for controlling **WHEN** and **WHAT** messages from Kafka get
skipped and marked for processing at a later point in time.

The **Trigger Interface** allows you to define **WHEN** the spout will start marking messages for delayed processing,
and **WHEN** the spout will start processing messages that it previously skipped.

The **Filter Interface** allows you to define **WHAT** messages the spout will mark for delayed processing.

The spout implementation handles the rest for you!  It tracks your filter criteria as well as offsets within Kafka topics to know where it started and stopped filtering.  It then uses this metadata to replay only those messages which got filtered.

## How does it  _really_ work?

Lets define the major components of the Spout and give a brief explanation of what their role is.  Then we'll
build up how they all work together.

### Primary Components

[SidelineSpout](src/main/java/com/salesforce/storm/spout/sideline/SidelineSpout.java) - Implements Storm's spout interface.  Everything starts and stops here.

[Consumer](src/main/java/com/salesforce/storm/spout/sideline/kafka/Consumer.java) - This is the high-level Kafka consumer built ontop of [`KafkaConsumer`](https://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html) 
that manages consuming messages from a Kafka topic as well as maintaining consumer state information.  It wraps `KafkaConsumer`
giving it semantics that play nicely with Storm.  `KafkaConsumer` assumes messages from a given partition are always
consumed in order and processed in order.  As we know Storm provides no guarantee that tuples emitted into a topology
will get processed and acknowledged in order.  As a result tracking which messages
within your Kafka topic have or have not been processed is not entirely trivial, and this wrapper aims
to deal with that.

[VirtualSpout](src/main/java/com/salesforce/storm/spout/sideline/kafka/VirtualSpout.java) - Within `SidelineSpout`, you will have one or more `VirtualSpout` instances.
These encapsulate `Consumer` to consume messages from Kafka, adding on functionality to determine
which messages should be emitted into the topology, tracking those that the topology have acknowledged, and which have failed.   

[PersistenceAdapter](src/main/java/com/salesforce/storm/spout/sideline/persistence/PersistenceAdapter.java) - This provides a persistence layer for storing `SidelineSpout`'s metadata.  It stores things
such as consumer state/offsets for consuming, as well as metadata about sideline requests.

[SpoutCoordinator](src/main/java/com/salesforce/storm/spout/sideline/SpoutCoordinator.java) - The `SpoutCoordinator` is responsible for managing the threads that are running the various `VirtualSpout`'s needed for sidelining.

As `nextTuple()` is called on `SidelineSpout`, it asks `SpoutCoordinator` for the next kafka message that should be emitted. 
The `SpoutCoordinator` gets the next message from one of its many `VirtualSpout` instances.

As `fail()` is called on `SidelineSpout`, the `SpoutCoordinator` determines which `VirtualSpout` instance
the failed tuple originated from and passes it to the correct instance's `fail()` method.

As `ack()` is called on `SidelineSpout`, the `SpoutCoordinator` determines which `VirtualSpout` instance
the acked tuple originated from and passes it to the correct instance's `ack()` method.

[SpoutRunner](src/main/java/com/salesforce/storm/spout/sideline/coordinator/SpoutRunner.java) - `VirtualSpout` instances are always run within their own processing thread.  
`SpoutRunner` encapsulates `VirtualSpout` and manages/monitors the thead it runs within.

[SpoutMonitor](src/main/java/com/salesforce/storm/spout/sideline/coordinator/SpoutMonitor.java) - Monitors new `VirtualSpout` instances that are created when a sideline request is stopped and ensures that a `SpoutRunner` is created to for it.

[StartingTrigger](src/main/java/com/salesforce/storm/spout/sideline/trigger/StartingTrigger.java) - An interface that can be attached to `SidelineSpout` via `setStartingTrigger()` and receives an instance of a `SpoutTriggerProxy` which allows the trigger to start a sideline request.

[StoppingTrigger](src/main/java/com/salesforce/storm/spout/sideline/trigger/StoppingTrigger.java) - An interface that can be attached to `SidelineSpout` via `setStoppingTrigger()` and receives an instance of a `SpoutTriggerProxy` which allows the trigger to stop a sideline request.

### When the Topology Starts
When your topology is deployed with a `SidelineSpout` and it starts up, the SidelineSpout will first start the `SpoutMonitor`. The `SpoutMonitor` then
creates the *main* `VirtualSpout` instance (sometimes called the firehose).  This *main* `VirtualSpout` instance is always running within
the spout, and its job is to consume from your Kafka topic.  As it consumes messages from Kafka, it deserializes
them using your [`Deserializer`](src/main/java/com/salesforce/storm/spout/sideline/kafka/deserializer/Deserializer.java) implementation.  It then runs it thru a [`FilterChain`](src/main/java/com/salesforce/storm/spout/sideline/filter/FilterChain.java), which is a collection of
[`FilterChainStep`](src/main/java/com/salesforce/storm/spout/sideline/filter/FilterChainStep.java) objects.  These filters determine what messages should be *sidelined* and which should be emitted out.
When no sideline requests are active, the `FilterChain` is empty, and all messages consumed from
Kafka will be converted to Tuples and emitted to your topology.

### Starting Sideline Request
Your implemented [`StartingTrigger`](src/main/java/com/salesforce/storm/spout/sideline/trigger/StartingTrigger.java) will notify the `SpoutMonitor` that a new sideline request has been started.  The `SpoutMonitor`
will record the *main* `VirtualSpout`'s current offsets within the topic and record them with request via
your configured [PersistenceAdapter](src/main/java/com/salesforce/storm/spout/sideline/persistence/PersistenceAdapter.java) implementation. The `SidelineSpout` will then attach the `FilterChainStep` to the *main* `VirtualSpout` instance, causing a subset of its messages to be filtered out.  This means that messages matching that criteria will /not/ be emitted to Storm.

### Stoping Sideline Request
Your implemented[`StoppingTrigger`](src/main/java/com/salesforce/storm/spout/sideline/trigger/StoppingTrigger.java) will notify the `SpoutMonitor` that it would like to stop a sideline request.  The `SpoutMonitor`
will first determine which `FilterChainStep` was associated with the request and remove it from the *main* `VirtualSpout` instance's
`FilterChain`.  It will also record the *main* `VirtualSpout`'s current offsets within the topic and record them via your
configured `PersistenceAdapter` implementation.  At this point messages consumed from the Kafka topic will no longer be filtered.
The `SidelineSpout ` will create a new instance of `VirtualSpout` configured to start consuming from the offsets
recorded when the sideline request was started.  The `SidelineSpout ` will then take the `FilterChainStep` associated with the request and wrap it in [`NegatingFilterChainStep`](src/main/java/com/salesforce/storm/spout/sideline/filter/NegatingFilterChainStep.java) and attach it to the *main* VirtualSpout's `FilterChain`.  This means that the inverse of the `FilterChainStep` that was applied to main `VirtualSpout` will not be applied to the sideline's `VirtualSpout`. In other words, if you were filtering X, Y and Z off of the main `VirtualSpout`, the sideline `VirtualSpout` will filter *everything but X, Y and Z*. Lastly the new `VirtualSpout` will be handed off to the `SpoutMonitor` to be wrapped in `SpoutRunner` and started. Once the `VirtualSpout` has completed consuming the skipped offsets, it will automatically shut down.

### Stopping & Redeploying the topology?
The `SidelineSpout` has several moving pieces, all of which will properly handle resuming in the state that they were
when the topology was halted.  The *main* `VirtualSpout` will continue consuming from the last acked offsets within your topic.
Metadata about active sideline requests are retrieved via `PersistenceAdapter` and resumed on start, properly filtering
messages from being emitted into the topology.  Metadata aboutsideline requests that have been stopped, but not finished, are retrieved via `PersistenceAdapter`, and `VirtualSpout` instances are
created and will resume consuming messages at the last previously acked offsets.

# Getting started
## Dependencies
Using the default straight-out-of-the-box configuration, this spout has the following dependencies:
- [Apache Storm 1.0.x](https://storm.apache.org/) - This one should be self explanatory.
- [Apache Kafka 0.10.0.x](https://kafka.apache.org/) - The underlying kafka consumer is based on this version of the Kafka-Client library.
- [Zookeeper](https://zookeeper.apache.org/) - Metadata the spout tracks has to be persisted somewhere, by default we use Zookeeper.  This is not
a hard dependency as you can write your own [`PersistenceAdapter`](src/main/java/com/salesforce/storm/spout/sideline/persistence/PersistenceAdapter.java) implementation to store this metadata
any where you would like.  Mysql? Redis? Kafka? Sure!  Contribute an adapter to the project!

## Configuration

[SidelineSpoutConfig](src/main/java/com/salesforce/storm/spout/sideline/config/SidelineSpoutConfig.java) contains a complete list of configuration options.

### Required Configuration

Config Key   | Type | Description | Default Value |
------------ | ---- | ----------- | --------------
sideline_spout.kafka.topic | String | Defines which Kafka topic we will consume messages from. | *null*
sideline_spout.kafka.brokers | List\<String\> | Holds a list of Kafka Broker hostnames + ports in the following format: ["broker1:9092", "broker2:9092", ...] | *null*
sideline_spout.consumer_id_prefix | String | Defines a consumerId prefix to use for all consumers created by the spout.  This must be unique to your spout instance, and must not change between deploys. | *null*
sideline_spout.output_stream_id | String | Defines the name of the output stream tuples will be emitted out of. | "default"
sideline_spout.deserializer.class | String | Defines which Deserializer implementation to use. Should be a full classpath to a class that implements the Deserializer interface. | *null*
sideline_spout.persistence_adapter.class | String | Defines which PersistenceAdapter implementation to use.  Should be a full classpath to a class that implements the PersistenceAdapter interface. | *null* 

## Required Interface Implementations
### [Deserializer](src/main/java/com/salesforce/storm/spout/sideline/kafka/deserializer/Deserializer.java)
The `Deserializer` interface dictates how the kafka key and messages consumed from Kafka as byte[] gets transformed into a storm tuple. It also 
controls the naming of your output field(s).

```java
    /**
     * Deserializes bytes into a Storm Values object
     * A null return value from here will result in this message being ignored.
     *
     * @param topic - Represents what topic this message came from.
     * @param partition - Represents what partition this message came from.
     * @param offset - Represents what offset this message came from.
     * @param key - Byte array representing the key.
     * @param value - Byte array representing the value.
     * @return Values that should be emitted by the spout to the topology.
     */
    Values deserialize(final String topic, final int partition, final long offset, final byte[] key, final byte[] value);

    /**
     * Declares the output fields for the deserializer.
     * @return An instance of the fields
     */
    Fields getOutputFields();
```

For compatibility to Storm-Kafka's `Scheme` interface, you can instead extend [`AbstractScheme`](src/main/java/com/salesforce/storm/spout/sideline/kafka/deserializer/compat/AbstractScheme.java)
and use an existing implementation.

### [StartingTrigger](src/main/java/com/salesforce/storm/spout/sideline/trigger/StartingTrigger.java)
The `StartingTrigger` interface dictates how your running spout instance gets notified of new requests to filter and sideline
messages being consumed from Kafka.

```java
void setSidelineSpout(SpoutTriggerProxy spout);
```

### [StoppingTrigger](src/main/java/com/salesforce/storm/spout/sideline/trigger/StoppingTrigger.java)
The `StoppingTrigger` interface dictates how your running spout instance gets notified of new requests to remove a previously
started filter and start reprocessing any messages that were previously skipped.

```java
void setSidelineSpout(SpoutTriggerProxy spout);
```

### [FilterChainStep](src/main/java/com/salesforce/storm/spout/sideline/filter/FilterChainStep.java)
The `FilterChainStep` interface dictates how you want to filter messages being consumed from kafka.  These filters 
should be functional in nature, always producing the exact same results given the exact same message.  They should 
ideally not depend on outside services or contextual information that can change over time.  These steps will ultimately 
be serialized and stored with the `PersistenceAdapter` so it is very important to make sure they function idempotently
when the same message is passed into them.  If your `FilterChainStep` does not adhere to this behavior you will run
into problems when sidelines are stopped and their data is re-processed.  Having functional classes with initial state
is OK so long as that state can be serialized.  In other words, if you're storing data in the filter step instances
you should only do this if they can be serialized and deserialized without side effects.

```java
    /**
     * Inputs an object, performs some business logic on it and then returns the result.
     *
     * @param message The filter to be processed by this step of the chain
     * @return The resulting filter after being processed
     */
    boolean filter(Message message);
```

## Example Trigger Implementation

The starting and stopping triggers are responsible for telling the `SidelineSpout` when to sideline.  While they are technically **not** required for the `SidelineSpout` to function, this project doesn't provide much value without them.

Each project leveraging the `SidelineSpout` will likely have a unique set of triggers representing your specific use case.  The following is a theoretical example only.

`NumberFilter` expects messages whose first value is an integer and if that value matches, it is filtered.  Notice that this
filter guarantees the same same behavior will occur after being serialized and than deserialized later.

```java
public static class NumberFilter implements FilterChainStep {

    final private int number;

    public NumberFilter(final int number) {
        this.number = number;
    }

    public boolean filter(Message message) {
        Integer messageNumber = (Integer) message.getValues().get(0);
        // Filter them if they don't match, in other words "not" equals
        return messageNumber.equals(number);
    }
}
```

`PollingSidelineTrigger` runs every 30 seconds and simply swaps out number filters, slowly incrementing over time.  It uses the `NumberFilter` by including it in a `SidelineRequest`.  `PollingSidelineTrigger` implements both `StartingTrigger` and `StoppingTrigger`, but this is not required.  You can create separate implementations for your project.

```java
public class PollingSidelineTrigger implements StartingTrigger, StoppingTrigger {

    private boolean isOpen = false;

    private transient ScheduledExecutorService executor;

    private transient SpoutTriggerProxy sidelineSpout;

    @Override
    public void open(final Map config) {
        if (isOpen) {
            return;
        }

        isOpen = true;

        executor = Executors.newScheduledThreadPool(1);

        Poll poll = new Poll(sidelineSpout);

        executor.scheduleAtFixedRate(poll, 0, 30, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        if (!executor.isShutdown()) {
            executor.shutdown();
        }
    }

    @Override
    public void setSidelineSpout(SpoutTriggerProxy sidelineSpout) {
        this.sidelineSpout = sidelineSpout;
    }

    static class Poll implements Runnable {

        private SpoutTriggerProxy spout;
        private Integer number = 0;

        Poll(SpoutTriggerProxy spout) {
            this.spout = spout;
        }

        @Override
        public void run() {
            // Start a sideline request for the next number
            SidelineRequest startRequest = new SidelineRequest(
                new NumberFilter(number++)
            );
            spout.startSidelining(startRequest);

            // Stop a sideline request for the last number
            SidelineRequest stopRequest = new SidelineRequest(
                new NumberFilter(number - 1)
            );
            spout.stopSidelining(stopRequest);
        }
    }
}
```

## Optional Interfaces for Overachievers
For most use cases the above interfaces are all that are required to get going.  But..sometimes your use case requires
you to do do something just a little special or different.  If that's the case with you, we provide the following 
configurable interfaces to hopefully ease the pain.

### [PersistenceAdapter](src/main/java/com/salesforce/storm/spout/sideline/persistence/PersistenceAdapter.java)
This interface dictates how and where metadata gets stored such that it lives between topology deploys.
In an attempt to decouple this data storage layer from the spout, we have this interface.  Currently we have
one implementation backed by Zookeeper.

###### Configuration
Config Key   | Type | Description | Default Value |
------------ | ---- | ----------- | --------------
sideline_spout.persistence_adapter.class | String | Defines which PersistenceAdapter implementation to use.    Should be a full classpath to a class that implements the PersistenceAdapter interface. | *null* 

### [RetryManager](src/main/java/com/salesforce/storm/spout/sideline/retry/RetryManager.java)
Interface for handling failed tuples.  By creating an implementation of this interface you can control how the spout deals with tuples that have failed within the topology. Currently we have
three implementations bundled with the spout which should cover most standard use cases.

###### Configuration
Config Key   | Type | Description | Default Value |
------------ | ---- | ----------- | --------------
sideline_spout.retry_manager.class | String | Defines which RetryManager implementation to use.  Should be a full classpath to a class that implements the RetryManager interface. |"com.salesforce.storm.spout.sideline.kafka.retry.DefaultRetryManager"

### [MessageBuffer](src/main/java/com/salesforce/storm/spout/sideline/buffer/MessageBuffer.java)
This interface defines an abstraction around essentially a concurrent queue.  By creating an abstraction around the queue it allows for things like
implementing a "fairness" algorithm on the poll() method for pulling off of the queue. Using a straight ConcurrentQueue would provide FIFO semantics 
but with an abstraction round robin across kafka consumers could be implemented, or any other preferred scheduling algorithm.

*Note: This is getting into the nitty-gritty internals of the spout here, you would need a pretty special use case to mess around
with this one.*

###### Configuration
Config Key   | Type | Description | Default Value |
------------ | ---- | ----------- | --------------
sideline_spout.coordinator.tuple_buffer.class | String | Defines which MessageBuffer implementation to use. Should be a full classpath to a class that implements the MessageBuffer interface. | "com.salesforce.storm.spout.sideline.buffer.RoundRobinBuffer"

## Optional Interfaces Implementations

These implementations are provided with the sideline spout.  Defaults are noted below along with any additional required configuration.

### PersistenceAdapter Implementations
#### [ZookeeperPersistenceAdapter](src/main/java/com/salesforce/storm/spout/sideline/persistence/ZookeeperPersistenceAdapter.java)
This is the default implementation, it uses a Zookeeper cluster to persist the required metadata.

###### Configuration
Config Key   | Type | Description | Default Value |
------------ | ---- | ----------- | --------------
sideline_spout.persistence.zk_servers | List\<String\> | Holds a list of Zookeeper server Hostnames + Ports in the following format: ["zkhost1:2181", "zkhost2:2181", ...] | *null* 
sideline_spout.persistence.zk_root | String | Defines the root path to persist state under.  Example: "/sideline-consumer-state" | *null*

#### [InMemoryPersistenceAdapter](src/main/java/com/salesforce/storm/spout/sideline/persistence/InMemoryPersistenceAdapter.java)
This implementation only stores metadata within memory.  This is useful for tests, but has no real world use case as all state will be lost between topology deploys.

### RetryManager Implementations
#### [DefaultRetryManager](src/main/java/com/salesforce/storm/spout/sideline/retry/DefaultRetryManager.java)
This is the default implementation for the spout.  It attempts retries of failed tuples a maximum of `retry_limit` times.
After a tuple fails more than that, it will be "acked" or marked as completed and never tried again.
Each retry is attempted using a calculated back-off time period.  The first retry will be attempted after `initial_delay_ms` milliseconds.  Each attempt
after that will be retried at (`FAIL_COUNT` * `initial_delay_ms` * `delay_multiplier`) milliseconds OR `retry_delay_max_ms` milliseconds, which ever is smaller.

###### Configuration
Config Key   | Type | Description | Default Value |
------------ | ---- | ----------- | --------------
sideline_spout.retry_manager.retry_limit | int | Defines how many times a failed message will be replayed before just being acked.  A value of 0 means tuples will never be retried. A negative value means tuples will be retried forever. | -1
sideline_spout.retry_manager.delay_multiplier | double | Defines how quickly the delay increases after each failed tuple. A value of 2.0 means the delay between retries doubles.  eg. 4, 8, 16 seconds, etc. | 2.0
sideline_spout.retry_manager.initial_delay_ms | long | Defines how long to wait before retry attempts are made on failed tuples, in milliseconds. Each retry attempt will wait for (number_of_times_message_has_failed * initial_delay_ms * delay_multiplier).  Example: If a tuple fails 5 times, initial_delay_ms is set to 1000, and delay_multiplier is set to 3.0 -- it will wait at least (5 * 1000 * 3.0) milliseconds before the next retry attempt. | 2000
sideline_spout.retry_manager.retry_delay_max_ms | long | Defines an upper bound of the max delay time between retried a failed tuple. | 900000

#### [FailedTuplesFirstRetryManager](src/main/java/com/salesforce/storm/spout/sideline/retry/FailedTuplesFirstRetryManager.java)
This implementation will always retry failed tuples at the earliest chance it can.  No back-off strategy, no maximum times a tuple can fail.

#### [NeverRetryManager](src/main/java/com/salesforce/storm/spout/sideline/retry/NeverRetryManager.java)
This implementation will never retry failed messages.  One and done.

### MessageBuffer Implementations
#### [RoundRobinBuffer](src/main/java/com/salesforce/storm/spout/sideline/buffer/RoundRobinBuffer.java)
This is the default implementation, which is essentially round-robin.  Each `VirtualSpout` has its own queue that gets added to.  A very chatty
virtual spout will not block/overrun less chatty ones.  The `poll()` method will round robin through all the available
queues to get the next msg.
 
Internally this implementation makes use of a `BlockingQueue` so that an upper bound can be put on the queue size.
Once a queue is full, any producer attempting to put more messages onto the queue will block and wait
for available space in the queue.  This acts to throttle producers of messages.
Consumers from the queue on the other hand will never block attempting to read from a queue, even if its empty.
This means consuming from the queue will always be fast.
 
#### [FIFOBuffer](src/main/java/com/salesforce/storm/spout/sideline/buffer/FIFOBuffer.java)
This is a first in, first out implementation.  It has absolutely no "fairness" between VirtualSpouts or any kind of "scheduling."

### MetricsRecorder Implementations
The interface [`MetricsRecorder`](src/main/java/com/salesforce/storm/spout/sideline/metrics/MetricsRecorder.java) defines how to handle metrics that are gathered by the spout.  Implementations of this interface
should be ThreadSafe, as a single instance is shared across multiple threads. Presently there are two implementations packaged with the project.

#### [StormRecorder](src/main/java/com/salesforce/storm/spout/sideline/metrics/StormRecorder.java)
This implementation registers metrics with [Apache Storm's metrics system](http://storm.apache.org/releases/1.0.1/Metrics.html).  It will report metrics using the following
format: 

Type | Format 
-----|--------
Averages | AVERAGES.\<className\>.\<metricName\>
Counter | COUNTERS.\<className\>.\<metricName\>
Gauge | GAUGES.\<className\>.\<metricName\>
Timer | TIMERS.\<className\>.\<metricName\> 
 

#### [LogRecorder](src/main/java/com/salesforce/storm/spout/sideline/metrics/LogRecorder.java)
This implementation logs metrics to your logging system.


## Metrics
SidelineSpout collects metrics giving you insight to what is happening under the hood.  It collects
four types of metrics, Averages, Counters, Gauges, and Timers.
  
Type | Description
-----|------------
Average | Calculates average of all values submitted over a set time period.
Counter | Keeps a running count that gets reset back to zero on deployment.
Gauge | Reports the last value given for the metric.
Timer | Calculates how long on average, in milliseconds, an event takes.

Below is a list of metrics that are collected with the metric type and description.

Class | Key | Type | Description
------|-----|------|------------
SidelineSpout | start-sideline | Counter | How many `Start Sideline` requests have been received.
SidelineSpout | stop-sideline | Counter | How many `Stop Sideline` requests have been received.
VirtualSidelineSpout | `virtual-spout-id`.emit | Counter | Tuple emit count per VirtualSpout instance.
VirtualSidelineSpout | `virtual-spout-id`.ack | Counter | Tuple ack count per VirtualSpout instance.
VirtualSidelineSpout | `virtual-spout-id`.fail | Counter | Messages who have failed.
VirtualSidelineSpout | `virtual-spout-id`.filtered | Counter | Filtered messages per VirtualSpout instance.
VirtualSidelineSpout | `virtual-spout-id`.exceeded_retry_limit | Counter | Messages who have exceeded the maximum configured retry count.
VirtualSidelineSpout | `virtual-spout-id`.number_filters_applied | Gauge | How many Filters are being applied against the VirtualSpout instance.
VirtualSidelineSpout | `virtual-spout-id`.partitionX.totalMessages | Gauge | Total number of messages to be processed by the VirtualSpout for the given partition.
VirtualSidelineSpout | `virtual-spout-id`.partitionX.totalProcessed | Gauge | Number of messages processed by the VirtualSpout instance for the given partition.
VirtualSidelineSpout | `virtual-spout-id`.partitionX.totalUnprocessed | Gauge | Number of messages remaining to be processed by the VirtualSpout instance for the given partition.
VirtualSidelineSpout | `virtual-spout-id`.partitionX.percentComplete | Gauge | Percentage of messages processed out of the total for the given partition.
VirtualSidelineSpout | `virtual-spout-id`.partitionX.startingOffset | Gauge | The starting offset position for the given partition.
VirtualSidelineSpout | `virtual-spout-id`.partitionX.currentOffset | Gauge | The offset currently being processed for the given partition.
VirtualSidelineSpout | `virtual-spout-id`.partitionX.endingOffset | Gauge | The ending offset for the given partition.
SpoutMonitor | poolSize | Gauge | The max number of VirtualSpout instances that will be run concurrently.
SpoutMonitor | running | Gauge | The number of running VirtualSpout instances.
SpoutMonitor | queued | Gauge | The number of queued VirtualSpout instances.
SpoutMonitor | completed | Gauge | The number of completed VirtualSpout instances.

# Interesting Ideas and Questions
Just a collection of random ideas, or things we could do with this:
- Can we use sidelines to trigger re-processing arbitrary ranges of data?
- Can we use sidelines to trigger dynamically trigger dropping certain messages onto the floor?

# Releases & Changelog 
See [CHANGELOG.md](CHANGELOG.md) for full release changes.
