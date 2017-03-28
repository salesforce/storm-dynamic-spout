<a name="storm-sideline-spout"></a>
# Storm Sidelining Kafka Spout

## Purpose of this project
The purpose of this project is to provide a [Kafka (0.10.0.x)](https://kafka.apache.org/) based spout for [Apache Storm (1.0.x)](https://storm.apache.org/) that provides the ability
to dynamically "*sideline*" specific messages to be replayed at a later time based on a filter criteria.

Under normal circumstances this Spout works much like your typical [Kafka Spout](https://github.com/nathanmarz/storm-contrib/tree/master/storm-kafka) and
aims to be a drop in replacement for it.  This implementation differs in that it exposes trigger and 
filtering semantics when you build your topology which allow for sidelined messages to be skipped, and then
replayed at a later time.

### Example use cases
Wow! That sounds interesting...but when would I need this?

#### Multi-tenant processing
When reading a multi-tenant commit log you may want to postpone processing for a given tenant. Sidelining 
is an implementation of tracking when a request to postpone processing occurs, by a given filter criteria, 
and then resuming it once the circumstances that caused it to be postponed are changed.

#### Some other use case here
Surely we can come up with another use case.
 
## How does it work?
This spout implementation exposes 2 interfaces for controlling **WHEN** and **WHAT** get messages from Kafka get
skipped for processing at a later time.

The **Trigger Interface** allows you to define how you tell the Spout **WHEN** to start marking messages for delayed processing.

The **Filter Interface** allows you to define how you tell the Spout **WHAT** messages get marked for delayed processing.

# Getting started
## Configuration

# Metrics

# Customization
## Interfaces
### Interface A
### Interface B
### Interface C

# Releases & Changelog 



 
