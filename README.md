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
Wow! That sounds interesting...but when would I need this?

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
## Configuration

## Required Interface Implementations
### [Deserializer]()
#### Compatibility [Scheme]()
### [StartingTrigger]()
### [StoppingTrigger]()
### [FilterChainStep]()

## Optional Interfaces for Overachievers
### [PersistenceManager]()
### [RetryManager]()
### [TupleBuffer]()
### [MetricsRecorder]()

# Metrics

# Releases & Changelog 


