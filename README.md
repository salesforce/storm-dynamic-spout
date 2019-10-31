[![Build Status](https://travis-ci.org/salesforce/storm-dynamic-spout.svg?branch=master)](https://travis-ci.org/salesforce/storm-dynamic-spout)

*This project is under active development.*

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Getting Started](#getting-started)
- [Dynamic Spout Framework](#dynamic-spout-framework)
  - [Purpose of this project](#purpose-of-this-project)
  - [How does it work?](#how-does-it-work)
  - [Dependencies](#dependencies)
  - [How the Framework Works](#how-the-framework-works)
    - [Components](#components)
  - [Configuration](#configuration)
    - [Dynamic Spout Configuration Options](#dynamic-spout-configuration-options)
    - [Persistence Configuration Options](#persistence-configuration-options)
    - [Zookeeper Persistence Configuration Options](#zookeeper-persistence-configuration-options)
    - [Kafka Consumer Configuration Options](#kafka-consumer-configuration-options)
    - [Adding Metrics](#adding-metrics)
  - [Handlers](#handlers)
    - [SpoutHandler](#spouthandler)
    - [VirtualSpoutHandler](#virtualspouthandler)
  - [Metrics](#metrics)
    - [Dynamic Spout Metrics](#dynamic-spout-metrics)
    - [Kafka Metrics](#kafka-metrics)
- [Sidelining](#sidelining)
  - [Example Use case: Multi-tenant Processing](#example-use-case-multi-tenant-processing)
    - [The Filter](#the-filter)
    - [Starting a Sideline](#starting-a-sideline)
    - [Resuming a Sideline](#resuming-a-sideline)
    - [Resolving a Sideline](#resolving-a-sideline)
    - [Stopping & Redeploying the Topology](#stopping--redeploying-the-topology)
  - [Configuration](#configuration-1)
    - [Sideline Configuration Options](#sideline-configuration-options)
  - [Metrics](#metrics-1)
  - [Example Sideline Trigger](#example-sideline-trigger)
  - [Recipes](#recipes)
- [Contributing](#contributing)
  - [Submitting a Contribution](#submitting-a-contribution)
  - [Acceptance Criteria](#acceptance-criteria)
- [Other Notes](#other-notes)
  - [History](#history)
  - [Tests](#tests)
  - [Checkstyle](#checkstyle)
  - [README Configuration & Metrics Tables](#readme-configuration--metrics-tables)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Getting Started

To use this library add the following to your pom.xml:

```
<dependency>
    <groupId>com.salesforce.storm</groupId>
    <artifactId>dynamic-spout</artifactId>
    <version>0.10-SNAPSHOT</version>
</dependency>
```

_Note that snapshot releases may have API changes, it's best to check for the latest release version and use that._

In addition to the library we publish source jars and javadocs to maven central. You can find those [here](https://search.maven.org/#artifactdetails%7Ccom.salesforce.storm%7Cdynamic-spout%7C0.8.0%7Cjar).

This project uses recent versions of [Apache Storm](https://storm.apache.org/) and [Apache Kafka](https://kafka.apache.org/) releases, but this library should function fine using any Storm 1.0 release or higher and any Kafka release 0.10 or higher.  If you run into compatibility problems please let us know by [filing an issue on GitHub](https://github.com/salesforce/storm-dynamic-spout/issues).

# Dynamic Spout Framework

## Purpose of this project
This library contains a reusable set of components that can be used to build a system of spouts masked behind a single spout for Apache Storm. The library came out of the development of _Apache Kafka Sidelining_. During it's development it became clear that many of the components could serve as building blocks for a more general purpose framework. Not long after separating out those components did we put them to use on other stream processing applications.


## How does it work?
The `DynamicSpout` is a container of many `DelegateSpout` instances, which each handle processing messages from their defined `Consumer` and pass them into Apache Storm as a single stream. You can think of a `DelegateSpout` as a typical Storm spout, with the `DynamicSpout` being an orchestration level. The `DynamicSpout` conforms to Apache Storm's `IRichSpout` interface, and so from Storm's standpoint there is only one Spout, and that's the magic of the framework.


## Dependencies
The `DynamicSpout` framework has the following dependencies:
- [Apache Storm](https://storm.apache.org/): For all of the Storm interfaces this project is implemented against. This dependency is marked as _provided_ so you will need to specify the version of Storm that you want to use in your project some where on your classpath.
- [Apache Zookeeper](https://zookeeper.apache.org/): The framework is agnostic to where metadata is stored, and you can easily add different types of storage by implementing a [`PersistenceAdapter`](src/main/java/com/salesforce/storm/spout/dynamic/persistence/PersistenceAdapter.java). The framework ships with a Zookeeper implementation because Storm itself requires Zookeeper to track metadata and we figured it's the easiest to provide out of the box. That said, Zookeeper is listed as an optional dependency and so you will need to include this in your classpath as well.
- [Apache Curator](https://curator.apache.org/): As noted above, the framework ships with a Zookeeper `PersistenceAdapter` and we use Curator to make all of those interactions easy and painless. Curator is also an optional dependency, so if you're using Zookeeper you will need to specify the correct version of Curator to go with your Zookeeper dependency.
- [Apache Kafka](https://kafka.apache.org/): The framework is not tied to Kafka, but we've found that many of our implementations of the framework are. This is an optional dependency that you will want to include if you're using the Kafka based `Consumer` for example.


## How the Framework Works
When a Storm topology is deployed with a `DynamicSpout`, the `DynamicSpout` will first start the `SpoutCoordinator`. The `SpoutCoordinator` will watch for `DelegateSpout` instances that are added to it. A `DelegateSpout` is just an interface that resembles Storm's `IRichSpout`, but it's not meant to be run on it's own, only in the context of the framework. There is a default implementation of the `DelegateSpout` called the `VirtualSpout` that handles the mechanics of working with a `Consumer` like the one included for talking to Kafka.

You may be wondering how a `DelegateSpout` instance gets added to the `SpoutCoordinator` and the answer is usually by a `SpoutHandler` instance that is configured on the `DynamicSpout`. A `SpoutHandler` is a set of hooks into the `DynamicSpout` life cycle. It allows you to do work when a `DynamicSpout` instance opens or closes (the topology's start/stop lifecycle).  For example, a `SpoutHandler` may check a node in Zookeeper for data and create `VirtualSpout` instances based upon that data, which is similar to how the sidelining implement works (more on that later).

There is also a `VirtualSpoutHandler` that is tied to the `VirtualSpout` implementation of the `DelegateSpout`.  Similarly it provides hooks into the life cycle of an individual `VirtualSpout` instance inside of the `DynamicSpout`.

Beyond the basic building blocks there are a number of places where you can change the behavior of the `DynamicSpout` to meet your needs. They include changing the spout's retry behavior, adding filters for messages and customizing where the spout stores metadata, among other things. Out of the box the framework tries to provide reasonable defaults to get a project going, but as your project matures you may want to customize various aspects of the framework. 


### Components
Below are some of the interfaces and their default implementations that you can use to customize the behavior of the `DynamicSpout`. Most implementation don't need to touch the vast majority of these, however there are a few such as the `Deserializer` and `SpoutHandler` that you will likely need to create project-specific implementations.

[DefaultSpout](src/main/java/com/salesforce/storm/spout/dynamic/DefaultSpout.java): This is the core building block of the `DynamicSpout` framework, implementations of this interface are what handle passing data down into a Storm topology. The default implementation of this is the
[`VirtualSpout`](src/main/java/com/salesforce/storm/spout/dynamic/VirtualSpout.java) which uses a `Consumer` model to consume messages. Most of the time the `VirtualSpout` is adequate enough for most implementations of the framework.

[Consumer](src/main/java/com/salesforce/storm/spout/dynamic/consumer/Consumer.java): A `Consumer` is used by a `VirtualSpout` instance to consume from a data source. The [Kafka](https://kafka.apache.org/) based [`Consumer`](src/main/java/com/salesforce/storm/spout/dynamic/kafka/Consumer.java) is an example implementation that polls for messages from a Kafka topic.

[Deserializer](src/main/java/com/salesforce/storm/spout/dynamic/kafka/deserializer/Deserializer.java): When using a `Consumer` it will need to know how to transform a message into a usable format. The framework ships with a  [`Utf8StringDeserializer`](src/main/java/com/salesforce/storm/spout/dynamic/kafka/deserializer/Utf8StringDeserializer.java) that merely ensures the bytes from a message are accessible as a `String`. If you're using structured messages like [Protocol Buffers](https://developers.google.com/protocol-buffers), [Apache Avro](https://avro.apache.org/) or even JSON you'll want to use a different deserializer.  This is very similar to Storm's [`Scheme`](https://github.com/apache/storm/blob/master/storm-client/src/jvm/org/apache/storm/spout/Scheme.java). If you're using anything other than plain text on the messages your `Consumer` is consuming from you'll probably be customizing this implementation.

[FilterChainStep](src/main/java/com/salesforce/storm/spout/dynamic/filter/FilterChainStep.java): A `VirtualSpout` supports applying filter to messages that come from the `Consumer`. This allows you to filter out messages at the spout level, before they ever get to your topology. Each filter is a step in a chain, so you can as many different filters as you want. This is a particularly powerful tool inside of the `SpoutHandler` and `VirtualSpoutHandler` where you can dynamically filter messages based upon how you're using the framework. These are completely optional and not required to use this framework.

[PersistenceAdapter](src/main/java/com/salesforce/storm/spout/dynamic/persistence/PersistenceAdapter.java): Storm spout's need a place to store metadata, like the position a `Consumer` is on a data source. Storm cluster's have a [Zookeeper](https://zookeeper.apache.org/) instance with them so the framework comes with a [`ZookeeperPersistenceAdapter`](src/main/java/com/salesforce/storm/spout/dynamic/persistence/ZookeeperPersistenceAdapter.java) out of the box for storing this data.  A `Consumer` such as the Kafka one provided with the framework would use this layer to track it's position on a topic's partition as it consumes data.

[RetryManager](src/main/java/com/salesforce/storm/spout/dynamic/retry/RetryManager.java): When a tuple fails in Storm the spout can attempt to retry it.  Different projects will have different retry needs, so the default implementation, [`ExponentialBackoffRetryManager`](src/main/java/com/salesforce/storm/spout/dynamic/retry/ExponentialBackoffRetryManager.java), mirrors the one in Storm itself by retrying a tuple with an exponential backoff. There are other implementations provided with the framework and if none of them support your needs you can implement your own. This is a more advanced customization of the framework that most projects will not need. 

[MessageBuffer](src/main/java/com/salesforce/storm/spout/dynamic/buffer/MessageBuffer.java): Because implementations fo the framework have many `DelegateSpout` instances emitting tuples downstream the `DynamicSpout` has a buffer that brings them altogether before passing them down to the topology. Out of the box the buffer is the [`RoundRobinBuffer`](src/main/java/com/salesforce/storm/spout/dynamic/buffer/RoundRobinBuffer.java) implementation, which loops through each virtual spout when it emits messages from the spout. Other implementations included in the framework are a first in, first out (FIFO) buffer as well as throttled and ratio based buffers that can emit tuples from different `DelegateSpouts` at variable rates. This is a more advanced customization that most projects will not need.

[MetricsRecorder](src/main/java/com/salesforce/storm/spout/dynamic/metrics/MetricsRecorder.java): There are a ton of metrics exposed by the `DynamicSpout` and you can customize how they are handled. By default the spout simply publishes metrics to logs using the [`LogRecorder`](src/main/java/com/salesforce/storm/spout/dynamic/metrics/LogRecorder.java). If you are using an older version of Storm (pre 1.2) you can use [`StormRecorder`](src/main/java/com/salesforce/storm/spout/dynamic/metrics/StormRecorder.java), or if you're using a more recent version of Storm checkout [`DropwizardRecorder`](src/main/java/com/salesforce/storm/spout/dynamic/metrics/DropwizardRecorder.java). All of the metrics captured by the `DynamicSpout` are listed in a section below. Additionally there is a section about how to create your own metrics in your customizations of the framework.

[SpoutHandler](src/main/java/com/salesforce/storm/spout/dynamic/handler/SpoutHandler.java): This is essential a set of hooks that can tie into the lifecycle events of a `DynamicSpout`, such as opening and closing of the `DynamicSpout`. Implementations of this interface can orchestrate the creation of `DelegateSpout` instances, and thus this is the primary integration point for people using the framework. While most every component provided with the framework has a default implementation, this one does not. That's because we expect this to be the point in the framework where your business logic for managing many spouts is to be built.

[VirtualSpoutHandler](src/main/java/com/salesforce/storm/spout/dynamic/handler/VirtualSpoutHandler.java): This is an optional set of hooks that can tie into the lifecycle events of a `VirtualSpout`. Note that an implementation of `DelegateSpout` does not necessarily support these, they are particular to the `VirtualSpout` or an implementation that explicitly supports them.  Similar to the `SpoutHandler`, implementations of the `VirtualSpoutHandler` allow for tieing into the lifecycle events of a `VirtualSpout`, such as as opening and closing of the `VirtualSpout`.


## Configuration
All of these options can be found inside of [`SpoutConfig`](src/main/java/com/salesforce/storm/spout/dynamic/config/SpoutConfig.java) and [`KafkaConsumerConfig`](src/main/java/com/salesforce/storm/spout/dynamic/kafka/KafkaConsumerConfig.java). They are defined using the [`ConfigDocumentation`](src/main/java/com/salesforce/storm/spout/documentation/ConfigDocumentation.java) on keys which are defined in their respective configuration classes. The [`DocGenerator`](src/main/java/com/salesforce/storm/spout/documentation/DocGenerator.java) then compiles them together below. 

<!-- DYNAMIC_SPOUT_CONFIGURATION_BEGIN_DELIMITER -->
### Dynamic Spout Configuration Options
Config Key | Type | Required | Description | Default Value |
---------- | ---- | -------- | ----------- | ------------- |
spout.consumer.class | String |  | Defines which Consumer implementation to use. Should be a full classpath to a class that implements the Consumer interface. | com.salesforce.storm.spout.dynamic.kafka.Consumer
spout.coordinator.consumer_state_flush_interval_ms | Long |  | How often we'll make sure each VirtualSpout persists its state, in Milliseconds. | 30000
spout.coordinator.max_concurrent_virtual_spouts | Integer |  | The size of the thread pool for running virtual spouts. | 10
spout.coordinator.max_spout_shutdown_time_ms | Long |  | How long we'll wait for all VirtualSpout's to cleanly shut down, before we stop them with force, in Milliseconds. | 10000
spout.coordinator.monitor_thread_interval_ms | Long |  | How often our monitor thread will run and watch over its managed virtual spout instances, in milliseconds. | 2000
spout.coordinator.tuple_buffer.class | String |  | Defines which MessageBuffer implementation to use. Should be a full classpath to a class that implements the MessageBuffer interface. | com.salesforce.storm.spout.dynamic.buffer.RoundRobinBuffer
spout.coordinator.tuple_buffer.max_size | Integer |  | Defines maximum size of the tuple buffer.  After the buffer reaches this size the internal VirtualSpouts will be blocked from generating additional tuples until they have been emitted into the topology. | 2000
spout.coordinator.virtual_spout_id_prefix | String |  | Defines a VirtualSpoutId prefix to use for all VirtualSpouts created by the spout. This must be unique to your spout instance, and must not change between deploys. | 
spout.metrics.class | String |  | Defines which MetricsRecorder implementation to use. Should be a full classpath to a class that implements the MetricsRecorder interface. | com.salesforce.storm.spout.dynamic.metrics.LogRecorder
spout.metrics.enable_task_id_prefix | Boolean |  | Defines if MetricsRecorder instance should include the taskId in the metric key. | 
spout.metrics.time_bucket | Integer |  | Defines the time bucket to group metrics together under. | 
spout.output_fields | List |  | Defines the output fields that the spout will emit as a list of field names. | 
spout.output_stream_id | String |  | Defines the name of the output stream tuples will be emitted out of. | default
spout.permanently_failed_output_stream_id | String |  | Defines the name of the output stream tuples that have permanently failed be emitted out of. | failed
spout.retry_manager.class | String | Required | Defines which RetryManager implementation to use. Should be a full classpath to a class that implements the RetryManager interface. | com.salesforce.storm.spout.dynamic.retry.ExponentialBackoffRetryManager
spout.retry_manager.delay_multiplier | Double |  | Defines how quickly the delay increases after each failed tuple. Example: A value of 2.0 means the delay between retries doubles.  eg. 4, 8, 16 seconds, etc. | 
spout.retry_manager.initial_delay_ms | Long |  | Defines how long to wait before retry attempts are made on failed tuples, in milliseconds. Each retry attempt will wait for (number_of_times_message_has_failed * min_retry_time_ms). Example: If a tuple fails 5 times, and the min retry time is set to 1000, it will wait at least (5 * 1000) milliseconds before the next retry attempt. | 1000
spout.retry_manager.retry_delay_max_ms | Long |  | Defines an upper bound of the max delay time between retried a failed tuple. | 
spout.retry_manager.retry_limit | Integer |  | Defines how many times a failed message will be replayed before just being acked. A negative value means tuples will be retried forever. A value of 0 means tuples will never be retried. A positive value means tuples will be retried up to this limit, then dropped. | 25
spout.spout_handler_class | String |  | Defines which SpoutHandler implementation to use. Should be a fully qualified class path that implements the SpoutHandler interface. | com.salesforce.storm.spout.dynamic.handler.NoopSpoutHandler
spout.virtual_spout_factory_class | String |  | Defines which DelegateSpoutFactory implementation to use. Should be a fully qualified class path that implements the DelegateSpoutFactory interface. | class com.salesforce.storm.spout.dynamic.VirtualSpoutFactory
spout.virtual_spout_handler_class | String |  | Defines which VirtualSpoutHandler implementation to use. Should be a fully qualified class path that implements the VirtualSpoutHandler interface. | com.salesforce.storm.spout.dynamic.handler.NoopVirtualSpoutHandler

### Persistence Configuration Options
Config Key | Type | Required | Description | Default Value |
---------- | ---- | -------- | ----------- | ------------- |
spout.persistence_adapter.class | String | Required | Defines which PersistenceAdapter implementation to use. Should be a full classpath to a class that implements the PersistenceAdapter interface. | 

### Zookeeper Persistence Configuration Options
Config Key | Type | Required | Description | Default Value |
---------- | ---- | -------- | ----------- | ------------- |
spout.persistence.zookeeper.connection_timeout | Integer |  | Zookeeper connection timeout. | 6000
spout.persistence.zookeeper.retry_attempts | Integer |  | Zookeeper retry attempts. | 10
spout.persistence.zookeeper.retry_interval | Integer |  | Zookeeper retry interval. | 10
spout.persistence.zookeeper.root | String |  | Defines the root path to persist state under. Example: "/consumer-state" | 
spout.persistence.zookeeper.servers | List |  | Holds a list of Zookeeper server Hostnames + Ports in the following format: ["zkhost1:2181", "zkhost2:2181", ...] | 
spout.persistence.zookeeper.session_timeout | Integer |  | Zookeeper session timeout. | 6000

<!-- DYNAMIC_SPOUT_CONFIGURATION_END_DELIMITER -->

<!-- KAFKA_CONSUMER_CONFIGURATION_BEGIN_DELIMITER -->
### Kafka Consumer Configuration Options
Config Key | Type | Required | Description | Default Value |
---------- | ---- | -------- | ----------- | ------------- |
spout.coordinator.virtual_spout_id_prefix | String |  | Defines a consumerId prefix to use for all consumers created by the spout. This must be unique to your spout instance, and must not change between deploys. | 
spout.kafka.brokers | List |  | Holds a list of Kafka Broker hostnames + ports in the following format: ["broker1:9092", "broker2:9092", ...] | 
spout.kafka.deserializer.class | String |  | Defines which Deserializer (Schema?) implementation to use. Should be a full classpath to a class that implements the Deserializer interface. | 
spout.kafka.topic | String |  | Defines which Kafka topic we will consume messages from. | 

<!-- KAFKA_CONSUMER_CONFIGURATION_END_DELIMITER -->

### Adding Metrics

Metrics provide several ways of numerical tracking data. These are very similar to the sort of stats you would use with Statd. The metric (or stat) types that are supported are:

Type | Format | Description
-----|------- | -------------- 
Averages | AVERAGES.\<className\>.\<metricName\> | Calculates average of all values submitted over a set time period.
Counter | COUNTERS.\<className\>.\<metricName\> | Keeps a running count that gets reset back to zero on deployment.
Gauge | GAUGES.\<className\>.\<metricName\> | Reports the last value given for the metric.
Timer | TIMERS.\<className\>.\<metricName\> | Calculates how long on average, in milliseconds, an event takes.  These metrics also publish a related counter

These can be handled through methods on an implementation of [`MetricsRecorder`](src/main/java/com/salesforce/storm/spout/dynamic/metrics/MetricsRecorder.java).

To track a specific metric you'll need to create a definition of it, using an implementation of [`MetricDefinition`](src/main/java/com/salesforce/storm/spout/dynamic/metrics/MetricDefinition.java). A `MetricDefinition` is pretty open ended, it just provides a consistent way from generating a String (called a key) to identify the metric. The framework itself uses [`ClassMetric`](src/main/java/com/salesforce/storm/spout/dynamic/metrics/ClassMetric.java) for it's metrics, however
 [`CustomMetric`](src/main/java/com/salesforce/storm/spout/dynamic/metrics/CustomMetric.java) is also provided.  A `ClassMetric` is specifically tied to a Java class and that classes name will be used to generate the key for the metric. This is a handy way of cataloging metrics, but you're free to do whatever is best for your project. All of the framework's core metrics are properties on [`SpoutMetrics`](src/main/java/com/salesforce/storm/spout/dynamic/metrics/SpoutMetrics.java) which is a handy, but not required, way of organizing your metrics.

Here's an example metric from `SpoutMetrics` in the framework:
```java
public static final MetricDefinition SPOUT_COORDINATOR_BUFFER_SIZE = new ClassMetric(SpoutCoordinator.class, "bufferSize");
```

This metric is then easily used by the `MetricsRecorder` like so:
```java
getMetricsRecorder().assignValue(SpoutMetrics.SPOUT_COORDINATOR_BUFFER_SIZE, getVirtualSpoutMessageBus().messageSize());
```

You can add new metrics to any custom implementation in the framework as needed. Most interfaces have an `open()` method that will receive a `MetricsRecorder` as a parameter. _If they don't, then this is a great opportunity for a contribution!_

Lastly you'll note that all of our metric keys are annotated with [`MetricDocumentation`](src/main/java/com/salesforce/storm/spout/documentation/MetricDocumentation.java), this is purely a convention of the framework which helps update the table of metrics below. If you're interested in how this is done, or want to do something similar check out the [`DocGenerator`](src/main/java/com/salesforce/storm/spout/documentation/DocGenerator.java) which compiles them together. 


## Handlers
Handlers essentially hooks that are attached to either the `DynamicSpout` and `VirtualSpout` and provide a way for interacting with their lifecycle stages without having to extend a base class. They serve as the key manner in which one might implement the framework in their project.

### SpoutHandler
The [`SpoutHandler`](src/main/java/com/salesforce/storm/spout/dynamic/handler/SpoutHandler.java) is an interface which allows you to tie into the `DynamicSpout` lifecycle.  Without a class implementing this interface the `DynamicSpout` in and of itself does not offer much value, as the `DynamicSpout` does not by itself know how to create `VirtualSpout` instances.  Your `SpoutHandler` implementation will be responsible for creating `VirtualSpout`'s and passing them back to the `DynamicSpout`'s coordinator.

There are several methods on the `SpoutHandler` you can implement. There are no-op defaults provided so you do not have to implement any of them, but there are four in particular we're going to go into detail because they are critical for most `SpoutHandler` implementations.

- `void open(Map<String, Object> spoutConfig)` - This method is functionally like the `SpoutHandler`'s constructor, it is called to setup the `SpoutHandler`. Just as `open()` exists on the `ISpout` interface in Storm to setup your spout, so this method is intended for setting up your `SpoutHandler`.
- `void close()` - This method is similar to an `ISpout`'s `close()` method, it gets called when the `SpoutHandler` is torn down, and you can use it shut down any classes that you have used in the `SpoutHandler` as well as clean up any object references you have.
- `void onSpoutOpen(DynamicSpout spout, Map topologyConfig, TopologyContext topologyContext)` - This method is called after the `DynamicSpout` is opened, and with it you get the `DynamicSpout` instance to interact with.  It's here that you can do things like call `DynamicSpout.addVirtualSpout(VirtualSpout virtualSpout)` to add a new `VirtualSpout` instance into the `DynamicSpout`.
- `void onSpoutClose(DynamicSpout spout)` - This method is called after `DynamicSpout` is closed, you can use it to perform shut down tasks when the `DynamicSpout` itself is closing, and with it you get the `DynamicSpout` instance to interact with.
- `void onSpoutActivate(DynamicSpout spout)` - This method is called `DynamicSpout` has been activated.
- `void onSpoutDectivate(DynamicSpout spout)` - This method is called `DynamicSpout` has been deactivated.

_It's important to note that `SpoutHandler` instance methods **should be blocking** since they are part of the startup and shutdown flow. Only perform asynchronous tasks if you are certain that other spout methods can be called without depending on your asynchronous tasks to complete._

Here is a sample `SpoutHandler` that can be used in conjunction with the Kafka `Consumer` to read a Kafka topic:
<!-- TODO: Can this be moved to an actual java class that is then embedded here? -->
```java
package com.salesforce.storm.spout.dynamic.example;

import com.salesforce.storm.spout.dynamic.DynamicSpout;
import com.salesforce.storm.spout.dynamic.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.VirtualSpout;
import com.salesforce.storm.spout.dynamic.consumer.ConsumerPeerContext;
import com.salesforce.storm.spout.dynamic.handler.SpoutHandler;

import org.apache.storm.task.TopologyContext;

import java.util.Map;

public class SimpleKafkaSpoutHandler implements SpoutHandler {

    @Override
    public void onSpoutOpen(DynamicSpout spout, Map topologyConfig, TopologyContext topologyContext) {
        // Create our main VirtualSpout that will consume off Kafka (note, you must have the Kafka Consumer configured)
        spout.addVirtualSpout(
            new VirtualSpout(
                // Unique identifier for this spout
                new DefaultVirtualSpoutIdentifier("kafkaSpout"),
                spout.getSpoutConfig(),
                // Tells the consumer of a VirtualSpout how many spout instances there are and which one this is
                new ConsumerPeerContext(1, 0),
                spout.getFactoryManager(),
                null, // Optional Starting ConsumerState
                null // Optional Ending ConsumerState
            )
        );
    }
}
```

### VirtualSpoutHandler
The [VirtualSpoutHandler](src/main/java/com/salesforce/storm/spout/dynamic/handler/VirtualSpoutHandler.java) is an interface which allows you to tie into the `VirtualSpout` lifecycle. An implementation is *not required* to use the dynamic spout framework, but it can be helpful when your implementation requires you to tap into the lifecycle of each individual spout being managed by the `DynamicSpout`.

There are several methods on the `VirtualSpoutHandler` you can implement. There are no-op defaults provided so you do not have to implement any of them, but there are five in particular we're going to go into detail because they are critical for most `VirtualSpoutHandler` implementations.

- `void open(Map<String, Object> spoutConfig)` - This method is functionally like the `VirtualSpoutHandler`'s constructor, it is called to setup the `VirtualSpoutHandler`. Just as `open()` exists in the `ISpout` interface in Storm to setup your spout, so this method is intended for setting up your `VirtualSpoutHandler`.
- `void close()` - This method is similar to an `ISpout`'s `close()` method, it gets called when the `VirtualSpoutHandler` is torn down, and you can use it shut down any classes that you have used in the `VirtualSpoutHandler` as well as clean up any object references you have.
- `void onVirtualSpoutOpen(DelegateSpout virtualSpout)` - This method is called after the `VirtualSpout` is opened, and with it you get the `VirtualSpout` instance to interact with.
- `void onVirtualSpoutClose(DelegateSpout virtualSpout)` - This method is called after the `VirtualSpout` is closed, and with it you get the `VirtualSpout` instance to interact with.
- `void onVirtualSpoutCompletion(DelegateSpout virtualSpout)` - This method is called before `onVirtualSpoutClose()` *only* when the VirtualSpout instance is about to close and has *completed* it's work, meaning that the Consumer has reached the provided ending offset.

## Metrics

Below is a list of metrics that are collected with the metric type and description.

<!-- DYNAMIC_SPOUT_METRICS_BEGIN_DELIMITER -->
### Dynamic Spout Metrics
Key | Type | Unit | Description |
--- | ---- | ---- | ----------- |
SpoutCoordinator.bufferSize | GAUGE | Number | Size of internal MessageBuffer. | 
SpoutCoordinator.completed | GAUGE | Number | The number of completed VirtualSpout instances. | 
SpoutCoordinator.errored | GAUGE | Number | The number of errored VirtualSpout instances. | 
SpoutCoordinator.poolSize | GAUGE | Number | The max number of VirtualSpout instances that will be run concurrently. | 
SpoutCoordinator.queued | GAUGE | Number | The number of queued VirtualSpout instances. | 
SpoutCoordinator.running | GAUGE | Number | The number of running VirtualSpout instances. | 
VirtualSpout.{virtualSpoutIdentifier}.ack | COUNTER | Number | Tuple ack count per VirtualSpout instance. | 
VirtualSpout.{virtualSpoutIdentifier}.emit | COUNTER | Number | Tuple emit count per VirtualSpout instance. | 
VirtualSpout.{virtualSpoutIdentifier}.exceededRetryLimit | COUNTER | Number | Messages who have exceeded the maximum configured retry count per VirtualSpout instance. | 
VirtualSpout.{virtualSpoutIdentifier}.fail | COUNTER | Number | Tuple fail count per VirtualSpout instance. | 
VirtualSpout.{virtualSpoutIdentifier}.filtered | COUNTER | Number | Filtered messages per VirtualSpout instance. | 
VirtualSpout.{virtualSpoutIdentifier}.numberFiltersApplied | GAUGE | Number | How many Filters are being applied against the VirtualSpout instance. | 
VirtualSpout.{virtualSpoutIdentifier}.partition.{partition}.currentOffset | GAUGE | Number | The offset currently being processed for the given partition. | 
VirtualSpout.{virtualSpoutIdentifier}.partition.{partition}.endingOffset | GAUGE | Number | The ending offset for the given partition. | 
VirtualSpout.{virtualSpoutIdentifier}.partition.{partition}.percentComplete | GAUGE | Number | Percentage of messages processed out of the total for the given partition. | 
VirtualSpout.{virtualSpoutIdentifier}.partition.{partition}.startingOffset | GAUGE | Percent 0.0 to 1.0 | The starting offset position for the given partition. | 
VirtualSpout.{virtualSpoutIdentifier}.partition.{partition}.totalMessages | GAUGE | Number | Total number of messages to be processed by the VirtualSpout for the given partition. | 
VirtualSpout.{virtualSpoutIdentifier}.partition.{partition}.totalProcessed | GAUGE | Number | Number of messages processed by the VirtualSpout instance for the given partition. | 
VirtualSpout.{virtualSpoutIdentifier}.partition.{partition}.totalUnprocessed | GAUGE | Number | Number of messages remaining to be processed by the VirtualSpout instance for the given partition. | 

<!-- DYNAMIC_SPOUT_METRICS_END_DELIMITER -->

<!-- KAFKA_CONSUMER_METRICS_BEGIN_DELIMITER -->
### Kafka Metrics
Key | Type | Unit | Description |
--- | ---- | ---- | ----------- |
KafkaConsumer.topic.{topic}.partition.{partition}.currentOffset | GAUGE | Number | Offset consumer has processed. | 
KafkaConsumer.topic.{topic}.partition.{partition}.endOffset | GAUGE | Number | Offset for TAIL position in the partition. | 
KafkaConsumer.topic.{topic}.partition.{partition}.lag | GAUGE | Number | Difference between endOffset and currentOffset metrics. | 

<!-- KAFKA_CONSUMER_METRICS_END_DELIMITER -->


# Sidelining

The purpose of the sidelining project is to provide a way to skip processing for a subset of messages on a [Kafka](https://kafka.apache.org/) topic while allowing for them to be processed at a later time. The canonical use case for this project is a multi-tenant Kafka topic, where you want to pause processing for a single tenant for some period of time without pausing other tenants. This project is built upon the `DynamicSpout` framework as a set of `SpoutHandler` and `VirtualSpoutHandler` implementations that apply custom `FilterChainStep` instances when something needs to be sidelined and then removes them when a sideline should resume or resolve. 
 
The project is wrapped in a spout implementation called `SidelineSpout` which extends the `DynamicSpout` and configures the handlers up in an oppinionated way. It is intended to works much like a typical [KafkaSpout](https://github.com/apache/storm/tree/master/external/storm-kafka-client).

## Example Use case: Multi-tenant Processing
When consuming a multi-tenant commit log you may want to postpone processing for one or more tenants. Imagine  that a subset of your tenants database infrastructure requires downtime for maintenance.  Using the `KafkaSpout` implementation that comes with Storm you really only have two options to deal with this situation:

1. Stop your entire topology for all tenants while the maintenance is performed for the small subset of tenants.  

2. Filter the problematic tenants out at the beginning of your topology to avoid processing further downstream, then once maintenance is complete you can stop filtering the problematic tenants. If you're ambitious you can keep track of where you started and stopped filtering and then spin up a separate `KafkaSpout` instance to process only the problematic tenant between the offsets that marked applying and removing your filter.
 
 If you can afford downtime, than option 1 is an easy way to deal with the problem. If not then option 2 might be something you have to look at.  The whole situation becomes even more complicated if you have more than one problematic tenant at a time, and even more so still if they need to start and/or stop filtering at different times. That's a lot of complexity!  Sidelining actually aims to handle option 2 for you, but through automation and easy control semantics.
 
 Imagine wrapping up all of that complexity behind a spout so that the rest of your storm topology doesn't have to think about that problem space: that's sidelining!
 
 ## Dependencies
 
 All of the optional dependencies of the `DynamicSpout` framework are required, namely Apache Zookeeper, Apache Curator and Apache Kafka. Check out the `DynamicSpout` dependencies section for more information.  These all need to be specified in your
 
 ## Lifecycle of a Sideline
 
 A sideline is just a filter applied at a specific point in time, with a promise to come back to it at a later point. A given sideline has a lifecycle, it is started, resumed and finally resolved.  Sidelines are always derived from a special `VirtualSpout` called the _fire hose_. The fire hose is your main Kafka topic, and if everything is going well and you don't need to sideline anything you're consuming all of the messages from it in real time. A sideline starts as a filter on the fire hose and evolves from there.
 
 
 ### The Trigger
 An implementation of sidelining has what's called a [`SidelineTrigger`](src/main/java/com/salesforce/storm/spout/sideline/trigger/SidelineTrigger.java) which gets called when the [`SidelineSpoutHandler`](src/main/java/com/salesforce/storm/spout/sideline/handler/SidelineSpoutHandler.java) first opens. The `SidelineTrigger` is responsible for setting up whatever is necessary to notify the [`SidelineController`](src/main/java/com/salesforce/storm/spout/sideline/handler/SidelineController.java) to start, resume or resolve a sideline. The sideline spout does not have an opinion on what you should use to notify that a sideline lifecycle event should take place, however a usable recipe using Zookeeper watches is provided if you would like to use it (_hint: the authors use it in production today_).


### The Filter
When you sideline you are telling the spout to skip messages (for now) based upon some sort of criteria. This is done by defining a `FilterChainStep`, a construct from the `DynamicSpout`. A `FilterChainStep` has a single method called `filter()` which receives a `Message` and return true or false based upon whether or not the supplied message should be skipped. _If you go back to our example use case, you may create a real simple `FilterChainStep` that takes a tenant's identifier in it's constructor and then checks a property on a `Message` to see if that message belongs to the provided tenant._


### Starting a Sideline
When a `SidelineTrigger` receives a signal to start a sideline it comes in the form of a `SidelineRequest` which is really just a wrapper for a `FilterChainStep`, which is a primitive of the `DynamicSpout` framework. The `FilterChainStep` is a really simple interface that defines a `filter()` method. You will need to define a `FilterChainStep` implementation for your use case. Your `SidelineTrigger` than will send your `FilterChainStep` to the `SidelineController` which will promptly apply the filter to the _fire hose_ and mark the starting offset on the Kafka topic for where the filter was applied.  Once a sideline has been started you should no longer be receiving messages that would match your filter.


### Resuming a Sideline
When a `SidelineTrigger` receives a signal to resume a sideline it must receive an identifier for a started sideline.  That means after you start a sideline you should hang onto the id it provides somewhere for later reference (again if you take a look at the provided recipes you'll see how we do this in Zookeeper). Resuming a sideline does not change the _fire hose_, so messages will continue to be filtered there. Resuming does, however, spin up a new `VirtualSpout` that will consume from the same topic as the _fire hose_, but it's going to do the inverse of your filter and it's going to start from the point the sideline was started (in the past). This means you will have the same Kafka topic being processed in parallel while a sideline is resuming, it's just that one `VirtualSpout` is filtering out specific messages (the _fire hose_) and the other `VirtualSpout` is only processing those filtered messages (the _sideline_). This is a good use case of the `ThrottledMessageBuffer` or `RatioMessageBuffer` because it can allow your for your _sideline_ to emit messages at a different rate back into the topology.  
 
 
### Resolving a Sideline
When you're ready to be done sidelining you can resolve it, which means that an endpoint is marked on the sideline. Remember that up until now all we had was a starting point for the sideline, we had no defined a point on the Kafka topic to stop sidelining. Resolving a sideline does that, which means that the `VirtualSpout` for the sideline will eventually finish the work it needs to do. Once it's finished the `VirtualSpout` will be closed and the instance is cleaned up. At the same time that the end offset is recorded the filter that was on the _fire hose_ `VirtualSpout` will be removed, meaning that the _fire hose_ will now be processing all of the messages on the Kafka topic.


### Stopping & Redeploying the Topology
The `DynamicSpout` has several moving pieces, all of which will properly handle resuming in the state that they were when the topology was halted.  The _fire hose_ `VirtualSpout` will resume consuming from its last acked offset, and any active sideline requests will also be resumed when the topology starts back up. However you choose to implement sidelining it is important to have some sort of durable storage for requests. The recipes include a Zookeeper based trigger that allows for sideline requests to come in while a topology is down without losing them.


## Configuration

<!-- SIDELINE_CONFIGURATION_BEGIN_DELIMITER -->
### Sideline Configuration Options
Config Key | Type | Required | Description | Default Value |
---------- | ---- | -------- | ----------- | ------------- |
sideline.persistence.zookeeper.connection_timeout | Integer |  | Zookeeper connection timeout. | 6000
sideline.persistence.zookeeper.retry_attempts | Integer |  | Zookeeper retry attempts. | 10
sideline.persistence.zookeeper.retry_interval | Integer |  | Zookeeper retry interval. | 10
sideline.persistence.zookeeper.root | String |  | Defines the root path to persist state under. Example: "/consumer-state" | 
sideline.persistence.zookeeper.servers | List |  | Holds a list of Zookeeper server Hostnames + Ports in the following format: ["zkhost1:2181", "zkhost2:2181", ...] | 
sideline.persistence.zookeeper.session_timeout | Integer |  | Zookeeper session timeout. | 6000
sideline.persistence_adapter.class | String | Required | Defines which PersistenceAdapter implementation to use. Should be a full classpath to a class that implements the PersistenceAdapter interface. | 
sideline.refresh_interval_seconds | Integer |  | Interval (in seconds) to check running sidelines and refresh them if necessary. | 600
sideline.trigger_class | String |  | Defines one or more sideline trigger(s) (if any) to use. Should be a fully qualified class path that implements thee SidelineTrigger interface. | 

<!-- SIDELINE_CONFIGURATION_END_DELIMITER -->

<!-- SIDELINE_METRICS_BEGIN_DELIMITER -->

## Metrics
Key | Type | Unit | Description |
--- | ---- | ---- | ----------- |
SidelineSpoutHandler.start | COUNTER | Number | Total number of started sidelines. | 
SidelineSpoutHandler.stop | COUNTER | Number | Total number of stopped sidelines. | 

<!-- SIDELINE_METRICS_END_DELIMITER -->


## Example Sideline Trigger

Each project leveraging the `SidelineSpout` will likely have a unique set of triggers representing your specific use case.  The following is a stripped down example intended to convey the concept of what a `SidelineTrigger` does. It's not a practical implementation and it's highly recommended that you refer to recipes package which includes a fully functional and production ready `SidelineTrigger`.

First we need a filter, so we'll use the `NumberFilter` which expects messages whose first value is an integer and if that value matches, it is filtered. Keep in mind your filter will be serialized to JSON and stored in Zookeeper (unless your change your `PersistenceAdapter`), so take great care to ensure that what you do serializes correctly.

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

`PollingSidelineTrigger` runs every 30 seconds and simply swaps out number filters, slowly incrementing over time.  It uses the `NumberFilter` by including it in a `SidelineRequest`.  `PollingSidelineTrigger` implements `SidelineTrigger`.

```java
public class PollingSidelineTrigger implements SidelineTrigger {

    private boolean isOpen = false;

    private transient ScheduledExecutorService executor;

    private transient SidelineController sidelineController;

    @Override
    public void open(final Map config) {
        if (isOpen) {
            return;
        }

        isOpen = true;

        executor = Executors.newScheduledThreadPool(1);

        final Poll poll = new Poll(sidelineSpout);

        executor.scheduleAtFixedRate(poll, 0, 30, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        if (!executor.isShutdown()) {
            executor.shutdown();
        }
    }

    @Override
    public void setSidelineController(SidelineController sidelineController) {
        this.sidelineController = sidelineController;
    }

    static class Poll implements Runnable {

        private SidelineController sidelineController;
        private Integer number = 0;

        Poll(SidelineController sidelineController) {
            this.sidelineController = sidelineController;
        }

        @Override
        public void run() {
            // Start a sideline request for the next number
            final SidelineRequest startRequest = new SidelineRequest(
                new NumberFilter(number++)
            );
            sidelineController.start(startRequest);

            if (number < 2) {
                return;
            }

            // Resume a sideline request for the last number
            final SidelineRequest resumeRequest = new SidelineRequest(
                new NumberFilter(number - 1)
            );
            sidelineController.resume(resumeRequest);

            if (number < 3) {
                return;
            }

            // Resolve a sideline request for two numbers back
            final SidelineRequest resolveRequest = new SidelineRequest(
                new NumberFilter(number - 2)
            );
            sidelineController.resume(resolveRequest);
        }
    }
}
```

## Recipes

The `SidelineSpout` offers a lot of places to customize it's behavior, but we've also tried to provide sensible defaults that work out of the box. In the case of a `SidelineTrigger` and the `FilterChainStep` necessary to sideline, though, these are often very specific to your businesses use case. Included in this project is a `recipes` package that includes a [`ZookeeperWatchTrigger`](src/main/java/com/salesforce/storm/spout/sideline/recipes/trigger/zookeeper/ZookeeperWatchTrigger.java) that is designed to use a `TriggerEvent` to signal starting, stopping and resuming a sideline. The actual trigger watches for these _events_ on a node in Zookeeper using the fantastic Curator `PathChildrenCacheListener`. This implementation sees changes to the node and than calls our listener, allowing for the handling of sidelines. We chose this as a recipe to bundle because we were doing this in our production uses of sidelining already and had great success with them. As mentioned elsewhere, the beauty of Zookeeper is every Storm cluster has it, and so it makes it easy to provide a reasonable default implementation to use.

If you want to give the recipe a try there is a minimal amount of configuration necessary:

```yaml
sideline.trigger_class: com.salesforce.storm.spout.sideline.recipes.trigger.zookeeper.ZookeeperWatchTrigger
## Most likely you will want to use your own custom filter here
sideline.filter_chain_step_class: com.salesforce.storm.spout.sideline.recipes.trigger.KeyFilter

## Zookeeper Watch Trigger configuration
sideline.zookeeper_watch_trigger.servers:
  - 127.0.0.1:2181
sideline.zookeeper_watch_trigger.root: /sideline-trigger
```

The recipe also includes a class called the [`TriggerEventHelper`](src/main/java/com/salesforce/storm/spout/sideline/recipes/trigger/TriggerEventHelper.java) which has easy to use methods like `startTriggerEvent()` that create the underlying events that make this trigger work.

You may be wondering why `TriggerEvent` instances when we already have `SidelineRequest` instances, and that's a good question!  The `TriggerEvent` provides additional operation data that, quite honestly, we don't care about in the actual sidelining process, like who performed this sideline action, what time it was done and what the reason for it was. For many use cases this is probably not necessary, but we found it helpful.

The other reason we chose to use yet another node in Zookeeper, a sort of staging ground for the sideline request, was because we wanted to allow for sidelines to occur when a topology was down. Good operational procedures should prevent humans from doing this, but we also recognize that sidelining might be handled by an automated process that may not have the awareness to tell if a topology is running or not.

All of this is to say, your use case might be far simpler then our own so buyer beware! But, this implementation is used today in production with great results.

Lastly, the `TriggerEvent` is not tightly coupled to the `ZookeeperWatchTrigger` and so we envision providing future recipes using different data stores and notification systems. Keep an eye out, and please feel free to contribute!

# Contributing

Found a bug? Think you've got an awesome feature you want to add? We welcome contributions!


## Submitting a Contribution

1. Search for an existing issue. If none exists, create a new issue so that other contributors can keep track of what you are trying to add/fix and offer suggestions (or let you know if there is already an effort in progress).  Be sure to clearly state the problem you are trying to solve and an explanation of why you want to use the strategy you're proposing to solve it.
1. Fork this repository on GitHub and create a branch for your feature.
1. Clone your fork and branch to your local machine.
1. Commit changes to your branch.
1. Push your work up to GitHub.
1. Submit a pull request so that we can review your changes.

*Make sure that you rebase your branch off of master before opening a new pull request. We might also ask you to rebase it if master changes after you open your pull request.*

## Acceptance Criteria

We love contributions, but it's important that your pull request adhere to some of the standards we maintain in this repository. 

- All tests must be passing!
- All code changes require tests!
- All code changes must be consistent with our checkstyle rules.
- New configuration options should have proper annotations and README updates generated.
- Great inline comments.

# Other Notes

## History

[Stephen Powis](https://github.com/crim) and [Stan Lemon](https://github.com/stanlemon) began this project in early February 2017 for [Salesforce](https://github.com/salesforce). By February 22nd we had split our project into its own git repository and rolled it out internally for several teams. In September 2017 we open sourced the project and then in November 2017 we split off some of our testing code for Kafka into it's own project [kafka-junit](https://github.com/salesforce/kafka-junit).

The idea of a dynamic spout framework was never something we set out to build. Sidelining existed before too, but in a much more complicated and high-resource fashion. We originally set out to re-design our sidelining implementation using a completely different strategy. In the course of things we realized there were a ton of reusable components in the sidelining project that might benefit from being decoupled, and so we started the process of surgically splitting the two. Out of this effort the dynamic spout framework was born, and it was almost serendipitous because we quickly had our first use case for managing many `VirtualSpout` instances behind a single interface. That effort involved a bunch of different http connections (one `VirtualSpout` for each) and while it looked nothing like sidelining it was a perfect fit for the dynamic spout framework.

## Tests

Extensive test coverage is available for both the `DynamicSpout` and `SidelineSpout` projects. There are both unit and functional tests. Today the functional tests are not clearly divided between the two projects (which is why they haven't been split up yet). This is simply an artifact of the way that project evolved.

You can run the tests by doing:
```
mvn clean test
```

## Checkstyle

We use checkstyle aggressively on source and tests, our config is located under the 'script' folder and can be imported into your IDE of choice.
 
## README Configuration & Metrics Tables

The configuration section in this document is generated using the [`DocGenerator`](src/main/java/com/salesforce/storm/spout/documentation/DocGenerator.java), which automatically generates the appropriate tables in this file using the  [`ConfigDocumentation`](src/main/java/com/salesforce/storm/spout/documentation/ConfigDocumentation.java) & [`MetricDocumentation`](src/main/java/com/salesforce/storm/spout/documentation/MetricDocumentation.java) annotations. Do **not** update those tables manually as they will get overwritten, instead use the `DocGenerator`, which can be executed easily from [`DocTask`](src/main/java/com/salesforce/storm/spout/dynamic/config/DocTask.java).
