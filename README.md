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
a separate topology/Kafka-Spout instance that knows where to start and stop consuming, and re-process the events for those 
tenants that were previously filtered.  

Unfortunately both of these solutions are complicated, error prone, and down right painful.

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

```
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

```
void setSidelineSpout(SpoutTriggerProxy spout);
```

### [StoppingTrigger](src/main/java/com/salesforce/storm/spout/sideline/trigger/StoppingTrigger.java)
The StoppingTrigger interface dictates how your running spout instance gets notified of new requests to remove a previously
started filter and start reprocessing any messages that were previously skipped.

```
void setSidelineSpout(SpoutTriggerProxy spout);
```

### [FilterChainStep](src/main/java/com/salesforce/storm/spout/sideline/filter/FilterChainStep.java)
The FilterChainStep interface dictates how you want to filter messages being consumed from kafka.

```
    /**
     * Inputs an object, performs some business logic on it and then returns the result.
     *
     * @param message The filter to be processed by this step of the chain
     * @return The resulting filter after being processed
     */
    boolean filter(KafkaMessage message);
```

## Optional Interfaces for Overachievers
### [PersistenceManager](src/main/java/com/salesforce/storm/spout/sideline/persistence/PersistenceManager.java)
#### Current Implementations
##### [ZookeeperPersistenceManager]()
##### [InMemoryPersistenceManager]()

### [RetryManager](src/main/java/com/salesforce/storm/spout/sideline/kafka/retryManagers/RetryManager.java)
#### Current Implementations
##### [DefaultRetryManager](src/main/java/com/salesforce/storm/spout/sideline/kafka/retryManagers/DefaultRetryManager.java)
##### [FailedTuplesFirstRetryManager](src/main/java/com/salesforce/storm/spout/sideline/kafka/retryManagers/FailedTuplesFirstRetryManager.java)
##### [NeverRetryManager](src/main/java/com/salesforce/storm/spout/sideline/kafka/retryManagers/NeverRetryManager.java)

### [TupleBuffer](src/main/java/com/salesforce/storm/spout/sideline/tupleBuffer/TupleBuffer.java)
#### Current Implementations
##### [RoundRobinBuffer](src/main/java/com/salesforce/storm/spout/sideline/tupleBuffer/RoundRobinBuffer.java)
##### [FIFOBuffer](src/main/java/com/salesforce/storm/spout/sideline/tupleBuffer/FIFOBuffer.java)

### [MetricsRecorder](src/main/java/com/salesforce/storm/spout/sideline/metrics/MetricsRecorder.java)
#### Current Implementations
##### [StormRecorder](src/main/java/com/salesforce/storm/spout/sideline/metrics/StormRecorder.java)
##### [LogRecorder](src/main/java/com/salesforce/storm/spout/sideline/metrics/LogRecorder.java)

# Metrics

# Releases & Changelog 


