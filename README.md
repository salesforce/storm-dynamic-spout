<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Dynamic Spout Framework](#dynamic-spout-framework)
  - [Purpose of this project](#purpose-of-this-project)
    - [Example use case: Multi-tenant processing](#example-use-case-multi-tenant-processing)
  - [How does it work?](#how-does-it-work)
  - [How does it  _really_ work?](#how-does-it--_really_-work)
  - [Dependencies](#dependencies)
  - [When the Topology Starts](#when-the-topology-starts)
  - [Configuration](#configuration)
    - [Sideline](#sideline)
    - [Kafka](#kafka)
    - [Persistence](#persistence)
    - [Zookeeper Persistence](#zookeeper-persistence)
  - [Components](#components)
  - [Provided Implementations](#provided-implementations)
    - [PersistenceAdapter Implementations](#persistenceadapter-implementations)
    - [RetryManager Implementations](#retrymanager-implementations)
    - [MessageBuffer Implementations](#messagebuffer-implementations)
    - [MetricsRecorder Implementations](#metricsrecorder-implementations)
  - [Handlers](#handlers)
    - [SpoutHandler](#spouthandler)
    - [VirtualSpoutHandler](#virtualspouthandler)
  - [Metrics](#metrics)
- [Sidelining](#sidelining)
  - [Getting Started](#getting-started)
  - [Starting Sideline Request](#starting-sideline-request)
  - [Stoping Sideline Request](#stoping-sideline-request)
  - [Dependencies](#dependencies-1)
  - [Components](#components-1)
  - [Example Trigger Implementation](#example-trigger-implementation)
  - [Stopping & Redeploying the topology?](#stopping--redeploying-the-topology)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Dynamic Spout Framework

## Purpose of this project
The purpose of this project is to create a reusable set of components that can be used to build a system of spouts masked behind a single spout for Apache Storm. The original use case for this framework was Apache Kafka Sidelining, but the framework was quickly found to have value in other stream processing contexts.

### Example use case: Multi-tenant processing
When consuming a multi-tenant commit log you may want to postpone processing for one or more tenants. Imagine  that a subset of your tenants database infrastructure requires downtime for maintenance.  Using the Kafka-Spout implementation you really only have two options to deal with this situation:

1. You could stop your entire topology for all tenants while the maintenance is performed for the small subset of tenants.  

2. You could filter these tenants out from being processed by your topology, then after the maintenance is complete start a separate topology/Kafka-Spout instance that somehow knows where to start and stop consuming, and by way filter bolts on the consuming topology re-process only the events for the tenants that were previously filtered.

Unfortunately both of these solutions are complicated, error prone and down right painful. The alternative is to represent a use case like this with a collection of spouts behind a single spout, or what we call a `VirtualSpout` instance behind a `DynamicSpout` that handled the management of starting and stopping those `VirtualSpout` instances.
 
## How does it work?
The `DynamicSpout` is really a container of many `VirtualSpout` instances, which each handle processing messages from their defined `Consumer` and pass them into Apache Storm as a single stream.

This spout implementation exposes two interfaces for controlling **WHEN** and **WHAT** messages from Kafka get skipped and marked for processing at a later point in time.

The **Trigger Interface** allows you to define **WHEN** the spout will start marking messages for delayed processing, and **WHEN** the spout will start processing messages that it previously skipped.

The **Filter Interface** allows you to define **WHAT** messages the spout will mark for delayed processing.

The spout implementation handles the rest for you!  It tracks your filter criteria as well as offsets within Kafka topics to know where it started and stopped filtering.  It then uses this metadata to replay only those messages which got filtered.

## How does it  _really_ work?
Lets define the major components of the `DynamicSpout` framework and give a brief explanation of what their role is.  Then we'll build up how they all work together.

## Dependencies
Using the default straight-out-of-the-box configuration, this spout has the following dependencies:
- [Apache Storm 1.0.x](https://storm.apache.org/) - This one should be self explanatory.
- [Zookeeper](https://zookeeper.apache.org/) - Metadata the spout tracks has to be persisted somewhere, by default we use Zookeeper.  This is not a hard dependency as you can write your own [`PersistenceAdapter`](src/main/java/com/salesforce/storm/spout/sideline/persistence/PersistenceAdapter.java) implementation to store this metadata any where you would like.  Mysql? Redis? Kafka? Sure!  Contribute an adapter to the project!

## When the Topology Starts
When your topology is deployed with a `DynamicSpout` and it starts up, the `DynamicSpout` will first start the `SpoutMonitor`. The `SpoutMonitor` will watch for `VirtualSpout` instances that are added to it, this is typically handled by a `SpoutHandler` instance that is configured on the `DynamicSpout`.  Each `VirtualSpout` will create a `Consumer` that leverages a starting `ConsumerState` to begin it's work.  
 
## Configuration
All of these options can be found inside of [SidelineSpoutConfig](src/main/java/com/salesforce/storm/spout/sideline/config/SidelineSpoutConfig.java).

[//]: <> (CONFIGURATION_BEGIN_DELIMITER)

Config Key | Type | Description | Default Value |
---------- | ---- | ----------- | ------------- |
Double |  | null | Defines how quickly the delay increases after each failed tuple. Example: A value of 2.0 means the delay between retries doubles.  eg. 4, 8, 16 seconds, etc.
Integer |  | 10 | The size of the thread pool for running virtual spouts for sideline requests.
Integer |  | 2000 | Defines maximum size of the tuple buffer.  After the buffer reaches this size the internal kafka consumers will be blocked from consuming.
Integer |  | 25 | Defines how many times a failed message will be replayed before just being acked. A negative value means tuples will be retried forever. A value of 0 means tuples will never be retried. A positive value means tuples will be retried up to this limit, then dropped.
Long |  | 1000 | Defines how long to wait before retry attempts are made on failed tuples, in milliseconds. Each retry attempt will wait for (number_of_times_message_has_failed * min_retry_time_ms). Example: If a tuple fails 5 times, and the min retry time is set to 1000, it will wait at least (5 * 1000) milliseconds before the next retry attempt.
Long |  | 10000 | How long we'll wait for all VirtualSpout's to cleanly shut down, before we stop them with force, in Milliseconds.
Long |  | 2000 | How often our monitor thread will run and watch over its managed virtual spout instances, in milliseconds.
Long |  | 30000 | How often we'll make sure each VirtualSpout persists its state, in Milliseconds.
Long |  | null | Defines an upper bound of the max delay time between retried a failed tuple.
String |  | com.salesforce.storm.spout.sideline.buffer.RoundRobinBuffer | Defines which MessageBuffer implementation to use. Should be a full classpath to a class that implements the MessageBuffer interface.
String |  | com.salesforce.storm.spout.sideline.kafka.Consumer | Defines which Consumer implementation to use. Should be a full classpath to a class that implements the Consumer interface.
String |  | com.salesforce.storm.spout.sideline.metrics.LogRecorder | Defines which MetricsRecorder implementation to use. Should be a full classpath to a class that implements the MetricsRecorder interface.
String |  | default | Defines the name of the output stream tuples will be emitted out of.
String | Required | com.salesforce.storm.spout.sideline.retry.DefaultRetryManager | Defines which RetryManager implementation to use. Should be a full classpath to a class that implements the RetryManager interface.

### Sideline
Config Key | Type | Description | Default Value |
---------- | ---- | ----------- | ------------- |
String |  | com.salesforce.storm.spout.sideline.handler.NoopSpoutHandler | Defines which SpoutHandler implementation to use. Should be a fully qualified class path that implements the SpoutHandler interface.
String |  | com.salesforce.storm.spout.sideline.handler.NoopVirtualSpoutHandler | Defines which VirtualSpoutHandler implementation to use. Should be a fully qualified class path that implements the VirtualSpoutHandler interface.
String |  | null | Defines with StartingTrigger (if any) implementation to use. Should be a fully qualified class path that implements thee StartingTrigger interface
String |  | null | Defines with StoppingTrigger (if any) implementation to use. Should be a fully qualified class path that implements thee StoppingTrigger interface

### Kafka
Config Key | Type | Description | Default Value |
---------- | ---- | ----------- | ------------- |
List |  | null | Holds a list of Kafka Broker hostnames + ports in the following format: ["broker1:9092", "broker2:9092", ...]
String |  | null | Defines a consumerId prefix to use for all consumers created by the spout. This must be unique to your spout instance, and must not change between deploys.
String |  | null | Defines which Deserializer (Schema?) implementation to use. Should be a full classpath to a class that implements the Deserializer interface.
String |  | null | Defines which Kafka topic we will consume messages from.

### Persistence
Config Key | Type | Description | Default Value |
---------- | ---- | ----------- | ------------- |
String | Required | null | Defines which PersistenceAdapter implementation to use. Should be a full classpath to a class that implements the PersistenceAdapter interface.

### Zookeeper Persistence
Config Key | Type | Description | Default Value |
---------- | ---- | ----------- | ------------- |
Integer |  | 10 | Zookeeper retry attempts.
Integer |  | 10 | Zookeeper retry interval.
Integer |  | 6000 | Zookeeper connection timeout.
Integer |  | 6000 | Zookeeper session timeout.
List |  | null | Holds a list of Zookeeper server Hostnames + Ports in the following format: ["zkhost1:2181", "zkhost2:2181", ...]
String |  | null | Defines the root path to persist state under. Example: "/sideline-consumer-state"


[//]: <> (CONFIGURATION_END_DELIMITER)

## Components
[DynamicSpout](src/main/java/com/salesforce/storm/spout/sideline/DynamicSpout.java) - Implements Storm's spout interface.  Everything starts and stops here.

[VirtualSpout](src/main/java/com/salesforce/storm/spout/sideline/VirtualSpout.java) - Within a `DynamicSpout`, you will have one or more `VirtualSpout` instances.  These encapsulate `Consumer` instances to consume messages from Kafka, adding on functionality to determine which messages should be emitted into the topology and tracking those that the topology have acknowledged or failed. 

[Consumer](src/main/java/com/salesforce/storm/spout/sideline/consumer/Consumer.java) - This is an interface which is used by the `VirtualSpout` to poll messages from a data source.

[SpoutCoordinator](src/main/java/com/salesforce/storm/spout/sideline/SpoutCoordinator.java) - The `SpoutCoordinator` is responsible for managing the threads that are running the various `VirtualSpout`'s needed for sidelining.

As `nextTuple()` is called on `DynamicSpout`, it asks `SpoutCoordinator` for the next message that should be emitted.  The `SpoutCoordinator` gets the next message from one of its many `VirtualSpout` instances.

As `fail()` is called on `DynamicSpout`, the `SpoutCoordinator` determines which `VirtualSpout` instance the failed tuple originated from and passes it to the correct instance's `fail()` method.

As `ack()` is called on `DynamicSpout`, the `SpoutCoordinator` determines which `VirtualSpout` instance the acked tuple originated from and passes it to the correct instance's `ack()` method.

[SpoutRunner](src/main/java/com/salesforce/storm/spout/sideline/coordinator/SpoutRunner.java) - `VirtualSpout` instances are always run within their own processing thread. `SpoutRunner` encapsulates `VirtualSpout` and manages/monitors the thead it runs within.

[SpoutMonitor](src/main/java/com/salesforce/storm/spout/sideline/coordinator/SpoutMonitor.java) - Monitors new `VirtualSpout` instances that are created when ensures that a `SpoutRunner` is created to for it.

[PersistenceAdapter](src/main/java/com/salesforce/storm/spout/sideline/persistence/PersistenceAdapter.java) - This provides a persistence layer for storing various metadata.  It stores consumer state, typically the offsets of te data source, that a given `VirtualSpout`'s `Consumer` has consumed. Currently we have three implementations bundled with the spout which should cover most standard use cases.

[SpoutHandler](src/main/java/com/salesforce/storm/spout/sideline/handler/SpoutHandler.java) - This interface can be implemented to interact with various lifecycle stages of the `DynamicSpout`. This class has access to the `DynamicSpout` instance and the ability to easily add new `VirtualSpout` instances to the `SpoutCoordinator`.  A `DynamicSpout` without a `SpoutHandler` won't do much in and of itself.

[VirtualSpoutHandler](src/main/java/com/salesforce/storm/spout/sideline/handler/VirtualSpoutHandler.java) - This interface can be implemented to interact with the various lifecycle stages of the `VirtualSpout`. This class has access to the `VirtualSpout` instance.

[PersistenceAdapter](src/main/java/com/salesforce/storm/spout/sideline/persistence/PersistenceAdapter.java) - This interface dictates how and where metadata gets stored such that it lives between topology deploys. In an attempt to decouple this data storage layer from the spout, we have this interface.  Currently we have one implementation backed by Zookeeper.

[RetryManager](src/main/java/com/salesforce/storm/spout/sideline/retry/RetryManager.java) - Interface for handling failed tuples.  By creating an implementation of this interface you can control how the spout deals with tuples that have failed within the topology. 

[MessageBuffer](src/main/java/com/salesforce/storm/spout/sideline/buffer/MessageBuffer.java) - This interface defines an abstraction around essentially a concurrent queue.  By creating an abstraction around the queue it allows for things like implementing a "fairness" algorithm on the poll() method for pulling off of the queue. Using a straight ConcurrentQueue would provide FIFO semantics but with an abstraction round robin across kafka consumers could be implemented, or any other preferred scheduling algorithm.

## Provided Implementations
The following implementations of previously mentioned interfaces are provided with the `DynamicSpout` framework.

### PersistenceAdapter Implementations
[ZookeeperPersistenceAdapter](src/main/java/com/salesforce/storm/spout/sideline/persistence/ZookeeperPersistenceAdapter.java) - This is the default implementation, it uses a Zookeeper cluster to persist the required metadata.

[InMemoryPersistenceAdapter](src/main/java/com/salesforce/storm/spout/sideline/persistence/InMemoryPersistenceAdapter.java) - This implementation only stores metadata within memory.  This is useful for tests, but has no real world use case as all state will be lost between topology deploys.

### RetryManager Implementations
[DefaultRetryManager](src/main/java/com/salesforce/storm/spout/sideline/retry/DefaultRetryManager.java) - This is the default implementation for the spout.  It attempts retries of failed tuples a maximum of `retry_limit` times. After a tuple fails more than that, it will be "acked" or marked as completed and never tried again. Each retry is attempted using a calculated back-off time period.  The first retry will be attempted after `initial_delay_ms` milliseconds.  Each attempt after that will be retried at (`FAIL_COUNT` * `initial_delay_ms` * `delay_multiplier`) milliseconds OR `retry_delay_max_ms` milliseconds, which ever is smaller.

[FailedTuplesFirstRetryManager](src/main/java/com/salesforce/storm/spout/sideline/retry/FailedTuplesFirstRetryManager.java) - This implementation will always retry failed tuples at the earliest chance it can.  No back-off strategy, no maximum times a tuple can fail.

[NeverRetryManager](src/main/java/com/salesforce/storm/spout/sideline/retry/NeverRetryManager.java) - This implementation will never retry failed messages.  One and done.

### MessageBuffer Implementations
[RoundRobinBuffer](src/main/java/com/salesforce/storm/spout/sideline/buffer/RoundRobinBuffer.java) - This is the default implementation, which is essentially round-robin.  Each `VirtualSpout` has its own queue that gets added to.  A very chatty virtual spout will not block/overrun less chatty ones.  The `poll()` method will round robin through all the available queues to get the next msg.
 
Internally this implementation makes use of a `BlockingQueue` so that an upper bound can be put on the queue size.
Once a queue is full, any producer attempting to put more messages onto the queue will block and wait
for available space in the queue.  This acts to throttle producers of messages.
Consumers from the queue on the other hand will never block attempting to read from a queue, even if its empty.
This means consuming from the queue will always be fast.
 
[FIFOBuffer](src/main/java/com/salesforce/storm/spout/sideline/buffer/FIFOBuffer.java) - This is a first in, first out implementation.  It has absolutely no "fairness" between VirtualSpouts or any kind of "scheduling."

### MetricsRecorder Implementations
The interface [`MetricsRecorder`](src/main/java/com/salesforce/storm/spout/sideline/metrics/MetricsRecorder.java) defines how to handle metrics that are gathered by the spout.  Implementations of this interface
should be ThreadSafe, as a single instance is shared across multiple threads. Presently there are two implementations packaged with the project.

[StormRecorder](src/main/java/com/salesforce/storm/spout/sideline/metrics/StormRecorder.java) - This implementation registers metrics with [Apache Storm's metrics system](http://storm.apache.org/releases/1.0.1/Metrics.html).  It will report metrics using the following format: 

Type | Format 
-----|--------
Averages | AVERAGES.\<className\>.\<metricName\>
Counter | COUNTERS.\<className\>.\<metricName\>
Gauge | GAUGES.\<className\>.\<metricName\>
Timer | TIMERS.\<className\>.\<metricName\> 

[LogRecorder](src/main/java/com/salesforce/storm/spout/sideline/metrics/LogRecorder.java) - This implementation logs metrics to your logging system.


## Handlers
Handlers are attached to the `DynamicSpout` and `VirtualSpout` and provide a way for interacting with the spout lifecycle without having to extend a base class.

### SpoutHandler
The [`SpoutHandler`](src/main/java/com/salesforce/storm/spout/sideline/handler/SpoutHandler.java) is an interface which allows you to tie into the `DynamicSpout` lifecycle.  Without a class implementing this interface the `DynamicSpout` in and of itself is pretty worthless, as the `DynamicSpout` does not by itself know how to create `VirtualSpout` instances.  Your `SpoutHandler` implementation will be responsible for creating `VirtualSpout`'s and passing them back to the `DynamicSpout`'s coordinator.

There are several methods on the `SpoutHandler` you can implement. There are no-op defaults provided so you do not have to implement any of them, but there are four in particular we're going to go into detail because they are critical for most `SpoutHandler` implementations.

- `void open(Map<String, Object> spoutConfig)` - This method is functionally like the `SpoutHandler`'s constructor, it is called to setup the `SpoutHandler`. Just as `open()` exists on the `ISpout` interface in Storm to setup your spout, so this method is intended for setting up your `SpoutHandler`.
- `void close()` - This method is similar to an `ISpout`'s `close()` method, it gets called when the `SpoutHandler` is torn down, and you can use it shut down any classes that you have used in the `SpoutHandler` as well as clean up any object references you have.
- `void onSpoutOpen(DynamicSpout spout, Map topologyConfig, TopologyContext topologyContext)` - This method is called after the `DynamicSpout` is opened, and with it you get the `DynamicSpout` instance to interact with.  It's here that you can do things like call `DynamicSpout.addVirtualSpout(VirtualSpout virtualSpout)` to add a new `VirtualSpout` instance into the `DynamicSpout`.
- `void onSpoutClose(DynamicSpout spout)` - This method is called after `DynamicSpout` is closed, you can use it to perform shut down tasks when the `DynamicSpout` itself is closing, and with it you get the `DynamicSpout` instance to interact with.

_It's important to note that `SpoutHandler` instance methods should be blocking since they are part of the startup and shutdown flow. Only perform asyncrhonous tasks if you are certain that other spout methods can be called without depending on your asyncrhonous tasks to complete._

Here is a sample `SpoutHandler` that can be used in conjunction with the Kafka `Consumer` to read a Kafka topic:

```java
import com.salesforce.storm.spout.sideline.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.DynamicSpout;
import com.salesforce.storm.spout.sideline.VirtualSpout;
import org.apache.storm.task.TopologyContext;

import java.util.Map;

public class SimpleKafkaSpoutHandler implements SpoutHandler {

    @Override
    public void onSpoutOpen(DynamicSpout spout, Map topologyConfig, TopologyContext topologyContext) {
        // Create our main VirtualSpout that will consume off Kafka (note, you must have the Kafka Consumer configured)
        VirtualSpout kafkaSpout = new VirtualSpout(
            spout.getSpoutConfig(),
            topologyContext,
            spout.getFactoryManager(),
            spout.getMetricsRecorder()
        );
        // Set a unique id that we can use to reference this VirtualSpout later
        kafkaSpout.setVirtualSpoutId(new DefaultVirtualSpoutIdentifier("kafkaSpout"));
        // Add it to the DynamicSpout, the SpoutCoordinator will handle spinning it up and tracking it from here
        spout.addVirtualSpout(kafkaSpout);
    }
}
```

### VirtualSpoutHandler
The [VirtualSpoutHandler](src/main/java/com/salesforce/storm/spout/sideline/handler/VirtualSpoutHandler.java) is an interface which allows you to tie into the `VirtualSpout` lifecycle. An implementation is *not required* to use the dynamic spout framework, but it can be helpful when your implementation requires you to tap into the lifecycle of each individual spout being managed by the `DynamicSpout`.

There are several methods on the `VirtualSpoutHandler` you can implement. There are no-op defaults provided so you do not have to implement any of them, but there are five in particular we're going to go into detail because they are critical for most `VirtualSpoutHandler` implementations.

- `void open(Map<String, Object> spoutConfig)` - This method is functionally like the `VirtualSpoutHandler`'s constructor, it is called to setup the `VirtualSpoutHandler`. Just as `open()` exists in the `ISpout` interface in Storm to setup your spout, so this method is intended for setting up your `VirtualSpoutHandler`.
- `void close()` - This method is similar to an `ISpout`'s `close()` method, it gets called when the `VirtualSpoutHandler` is torn down, and you can use it shut down any classes that you have used in the `VirtualSpoutHandler` as well as clean up any object references you have.
- `void onVirtualSpoutOpen(DelegateSpout virtualSpout)` - This method is called after the `VirtualSpout` is opened, and with it you get the `VirtualSpout` instance to interact with.
- `void onVirtualSpoutClose(DelegateSpout virtualSpout)` - This method is called after the `VirtualSpout` is closed, and with it you get the `VirtualSpout` instance to interact with.
- `void onVirtualSpoutCompletion(DelegateSpout virtualSpout)` - This method is called before `onVirtualSpoutClose()` *only* when the VirtualSpout instance is about to close and has *completed* it's work, meaning that the Consumer has reached the provided ending offset.

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
VirtualSpout | `virtual-spout-id`.emit | Counter | Tuple emit count per VirtualSpout instance.
VirtualSpout | `virtual-spout-id`.ack | Counter | Tuple ack count per VirtualSpout instance.
VirtualSpout | `virtual-spout-id`.fail | Counter | Messages who have failed.
VirtualSpout | `virtual-spout-id`.filtered | Counter | Filtered messages per VirtualSpout instance.
VirtualSpout | `virtual-spout-id`.exceeded_retry_limit | Counter | Messages who have exceeded the maximum configured retry count.
VirtualSpout | `virtual-spout-id`.number_filters_applied | Gauge | How many Filters are being applied against the VirtualSpout instance.
VirtualSpout | `virtual-spout-id`.partitionX.totalMessages | Gauge | Total number of messages to be processed by the VirtualSpout for the given partition.
VirtualSpout | `virtual-spout-id`.partitionX.totalProcessed | Gauge | Number of messages processed by the VirtualSpout instance for the given partition.
VirtualSpout | `virtual-spout-id`.partitionX.totalUnprocessed | Gauge | Number of messages remaining to be processed by the VirtualSpout instance for the given partition.
VirtualSpout | `virtual-spout-id`.partitionX.percentComplete | Gauge | Percentage of messages processed out of the total for the given partition.
VirtualSpout | `virtual-spout-id`.partitionX.startingOffset | Gauge | The starting offset position for the given partition.
VirtualSpout | `virtual-spout-id`.partitionX.currentOffset | Gauge | The offset currently being processed for the given partition.
VirtualSpout | `virtual-spout-id`.partitionX.endingOffset | Gauge | The ending offset for the given partition.
SpoutMonitor | poolSize | Gauge | The max number of VirtualSpout instances that will be run concurrently.
SpoutMonitor | running | Gauge | The number of running VirtualSpout instances.
SpoutMonitor | queued | Gauge | The number of queued VirtualSpout instances.
SpoutMonitor | completed | Gauge | The number of completed VirtualSpout instances.


# Sidelining

The purpose of this project is to provide a [Kafka (0.10.0.x)](https://kafka.apache.org/) based spout for [Apache Storm (1.0.x)](https://storm.apache.org/) that provides the ability to dynamically "*sideline*" or skip specific messages to be replayed at a later time based on a set of filter criteria.

Under normal circumstances this spout works much like your typical [Kafka-Spout](https://github.com/nathanmarz/storm-contrib/tree/master/storm-kafka) and aims to be a drop in replacement for it.  This implementation differs in that it exposes trigger and filter semantics when you build your topology which allow for specific messages to be skipped, and then replayed at a later point in time.  All this is done dynamically without requiring you to re-deploy your topology when filtering 
criteria changes!

 Sidelining uses the `DynamicSpout` framework and begins by creating the *main* `VirtualSpout` instance (sometimes called the firehose).  This *main* `VirtualSpout` instance is always running within the spout, and its job is to consume from your Kafka topic.  As it consumes messages from Kafka, it deserializes them using your [`Deserializer`](src/main/java/com/salesforce/storm/spout/sideline/kafka/deserializer/Deserializer.java) implementation.  It then runs it thru a [`FilterChain`](src/main/java/com/salesforce/storm/spout/sideline/filter/FilterChain.java), which is a collection of[`FilterChainStep`](src/main/java/com/salesforce/storm/spout/sideline/filter/FilterChainStep.java) objects.  These filters determine what messages should be *sidelined* and which should be emitted out. When no sideline requests are active, the `FilterChain` is empty, and all messages consumed from Kafka will be converted to Tuples and emitted to your topology.


## Getting Started
In order to begin using sidelining you will need to create a `FilterChainStep`, a `StartingTrigger` and `StoppingTrigger`, implementing classes are all required for this spout to function properly.

## Starting Sideline Request
Your implemented [`StartingTrigger`](src/main/java/com/salesforce/storm/spout/sideline/trigger/StartingTrigger.java) will notify the `SpoutMonitor` that a new sideline request has been started.  The `SpoutMonitor`
will record the *main* `VirtualSpout`'s current offsets within the topic and record them with request via
your configured [PersistenceAdapter](src/main/java/com/salesforce/storm/spout/sideline/persistence/PersistenceAdapter.java) implementation. The `SidelineSpout` will then attach the `FilterChainStep` to the *main* `VirtualSpout` instance, causing a subset of its messages to be filtered out.  This means that messages matching that criteria will /not/ be emitted to Storm.

## Stoping Sideline Request
Your implemented[`StoppingTrigger`](src/main/java/com/salesforce/storm/spout/sideline/trigger/StoppingTrigger.java) will notify the `SpoutMonitor` that it would like to stop a sideline request.  The `SpoutMonitor` will first determine which `FilterChainStep` was associated with the request and remove it from the *main* `VirtualSpout` instance's `FilterChain`.  It will also record the *main* `VirtualSpout`'s current offsets within the topic and record them via your configured `PersistenceAdapter` implementation.  At this point messages consumed from the Kafka topic will no longer be filtered. The `SidelineSpout ` will create a new instance of `VirtualSpout` configured to start consuming from the offsets recorded when the sideline request was started.  The `SidelineSpout ` will then take the `FilterChainStep` associated with the request and wrap it in [`NegatingFilterChainStep`](src/main/java/com/salesforce/storm/spout/sideline/filter/NegatingFilterChainStep.java) and attach it to the *main* VirtualSpout's `FilterChain`.  This means that the inverse of the `FilterChainStep` that was applied to main `VirtualSpout` will not be applied to the sideline's `VirtualSpout`. In other words, if you were filtering X, Y and Z off of the main `VirtualSpout`, the sideline `VirtualSpout` will filter *everything but X, Y and Z*. Lastly the new `VirtualSpout` will be handed off to the `SpoutMonitor` to be wrapped in `SpoutRunner` and started. Once the `VirtualSpout` has completed consuming the skipped offsets, it will automatically shut down.


## Dependencies
- DynamicSpout framework (which is an Apache Storm specific implementation)
- [Apache Kafka 0.10.0.x](https://kafka.apache.org/) - The underlying kafka consumer is based on this version of the Kafka-Client library.


## Components
[StartingTrigger](src/main/java/com/salesforce/storm/spout/sideline/trigger/StartingTrigger.java) - An interface that is configured and created by the `SidelineSpoutHandler` and will receive an instance of `SpoutTriggerProxy` via `setSidelineSpout()`.  This implementation can call `startSidelining()` with a `SidelineRequest`, which contains a `SidelineRequestIdentifier` and a `FilterChainStep` when a new sideline should be spun up.

[StoppingTrigger](src/main/java/com/salesforce/storm/spout/sideline/trigger/StoppingTrigger.java) - An interface that is configured and created by the `SidelineSpoutHandler` and will receive an instance of `SpoutTriggerProxy` via `setSidelineSpout()`.  This implementation can call `stopSidelining()` with a `SidelineRequest`, which contains a `SidelineRequestIdentifier` and a `FilterChainStep` when a sideline should be stopped, meaning that the firehose will no longer filter out events and new `VirtualSpout` will be created for the previously sidelined messages.


[Deserializer](src/main/java/com/salesforce/storm/spout/sideline/kafka/deserializer/Deserializer.java) - The `Deserializer` interface dictates how the kafka key and messages consumed from Kafka as byte[] gets transformed into a storm tuple. It also  controls the naming of your output field(s).  An example `Utf8StringDeserializer` is provided implementing this interface.

[FilterChainStep](src/main/java/com/salesforce/storm/spout/sideline/filter/FilterChainStep.java) - The `FilterChainStep` interface dictates how you want to filter messages being consumed from kafka.  These filters should be functional in nature, always producing the exact same results given the exact same message.  They should  ideally not depend on outside services or contextual information that can change over time.  These steps will ultimately  be serialized and stored with the `PersistenceAdapter` so it is very important to make sure they function idempotently when the same message is passed into them.  If your `FilterChainStep` does not adhere to this behavior you will run into problems when sidelines are stopped and their data is re-processed.  Having functional classes with initial state is OK so long as that state can be serialized.  In other words, if you're storing data in the filter step instances you should only do this if they can be serialized and deserialized without side effects.

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

## Stopping & Redeploying the topology?
The `DynamicSpout` has several moving pieces, all of which will properly handle resuming in the state that they were when the topology was halted.  The *main* `VirtualSpout` will continue consuming from the last acked offsets within your topic. Metadata about active sideline requests are retrieved via `PersistenceAdapter` and resumed on start, properly filtering messages from being emitted into the topology.  Metadata aboutsideline requests that have been stopped, but not finished, are retrieved via `PersistenceAdapter`, and `VirtualSpout` instances are created and will resume consuming messages at the last previously acked offsets.
