# Change Log
The format is based on [Keep a Changelog](http://keepachangelog.com/) and adheres to [Semantic Versioning](http://semver.org/).

## 0.10.0 (RELEASE DATE TBD)

### DynamicSpout Framework
#### Breaking Changes
- [PR-37](https://github.com/salesforce/storm-dynamic-spout/pull/37) Moved `FilterChain` back to the `dynamic` package.
- [PR-52](https://github.com/salesforce/storm-dynamic-spout/pull/52) Remove Kafka-Test-Server.  Replace with Kafka-JUnit external dependency.
- [PR-66](https://github.com/salesforce/storm-dynamic-spout/pull/66) DynamicSpout.open() now throws IllegalStateException if you attempt to open it more than once.

### Improvements
- [PR-38](https://github.com/salesforce/storm-dynamic-spout/pull/38) Removed unused method `Deserializer.getOutputFields()`.
- [PR-37](https://github.com/salesforce/storm-dynamic-spout/pull/37) Added hasStep() and getStep() to `FilterChain` and added a specific exception for serializing/deserializing `FilterChainStep` objects. 
- [PR-85](https://github.com/salesforce/storm-dynamic-spout/pull/85) Fix ordering bug in DefaultRetryManager. Always retry the earliest messageId.
- [PR-90](https://github.com/salesforce/storm-dynamic-spout/pull/90) Adds new 'Permanently Failed' output stream.  Tuples that exceed the configured retry limit will now be emitted out a 'failed' stream un-anchored. This allows you to do your own error handling within the topology by subscribing to this stream. The name of this output stream defaults to 'failed', but is configurable via the `spout.permanently_failed_output_stream_id` configuration item.
- [ISSUE-100](https://github.com/salesforce/storm-dynamic-spout/issues/100) Fix NPE in RatioMessageBuffer on Spout deploy.
- [PR-XX](#) Updates Kafka dependencies from 0.11.0.1 to 0.11.0.2.  Updates Kafka-JUnit dependency.

#### Removed
- [PR-45](https://github.com/salesforce/storm-dynamic-spout/pull/45) Removed getMaxLag() from Consumer interface.
- [PR-81](https://github.com/salesforce/storm-dynamic-spout/pull/81) Removed getPersistenceAdapter() from Consumer interface.

#### Improvements
- [PR-37](https://github.com/salesforce/storm-dynamic-spout/pull/37) Added hasStep() and getStep() to `FilterChain` and added a specific exception for serializing/deserializing FilterChainSteps.
- [PR-40](https://github.com/salesforce/storm-dynamic-spout/pull/40) Timer metrics will now publish a counter metric containing the total time spent within each metric key.  Start publishing developer performance metrics within VirtualSpout via MetricRecorder.
- [PR-45](https://github.com/salesforce/storm-dynamic-spout/pull/45) MetricRecorder is now passed into Consumer interface via open() method.
- [PR-55](https://github.com/salesforce/storm-dynamic-spout/pull/55) Refactored how DynamicSpout coordinates with VirtualSpouts internally.

### Sideline Framework
In this release the 'stop' sideline state was split into 'resume' and 'resolve', allowing for one to begin processing a sideline without designating an ending offset and then applying an ending offset later on. Based upon our experience this is a more practical division of state for sidelining.

#### Breaking Changes
- [PR-79](https://github.com/salesforce/storm-dynamic-spout/pull/79) Add support for resume state when sidelining.
- [PR-37](https://github.com/salesforce/storm-dynamic-spout/pull/37) Replaced `SpoutTriggerProxy` with `SidelineController` interface.

#### Removed
- [PR-37](https://github.com/salesforce/storm-dynamic-spout/pull/37) Removed deprecated code.
    - Remove deprecated constructor from SidelineRequest
    - Removed deprecated constructors from SidelineRequestIdentifier
    
#### Improvements
- [PR-37](https://github.com/salesforce/storm-dynamic-spout/pull/37) Added isSidelineStarted() and isSidelineStopped() to the `SidelineController`
- [PR-37](https://github.com/salesforce/storm-dynamic-spout/pull/37) When deserializing an invalid FilterChainStep it would crash the spout instance.
- [PR-37](https://github.com/salesforce/storm-dynamic-spout/pull/37) Replaced `SidelineRequestIdentifier` with `FilterChainStepIdentifier` in the FilterChain.
- [PR-37](https://github.com/salesforce/storm-dynamic-spout/pull/37) Added `is*()` methods to the `SidelineController` for the respective sideline states.
- [PR-47](https://github.com/salesforce/storm-dynamic-spout/pull/47) Sidelining periodically checks to ensure `VirtualSpout` instances are running and have the proper `FilterChainStep` objects applied to them.
- [PR-97](https://github.com/salesforce/storm-dynamic-spout/pull/97) Sideline config defaults are now set properly.
- [PR-99](https://github.com/salesforce/storm-dynamic-spout/pull/99) Do not pass empty ending state when loading sidelines.

### Kafka Consumer
#### Removed
- [PR-38](https://github.com/salesforce/storm-dynamic-spout/pull/38) Removed unused method Deserializer.getOutputFields()

#### Improvements
- [PR-45](https://github.com/salesforce/storm-dynamic-spout/pull/45) Added lag, currentOffset, endOffset metrics to Kafka Consumer.
- [PR-43](https://github.com/salesforce/storm-dynamic-spout/pull/43) Fixed unbounded recursion when calling fillBuffer() in the KafkaConsumer.
- [PR-98](https://github.com/salesforce/storm-dynamic-spout/pull/98) Allow null keys from Kafka in the Utf8Deserializer and handle null values properly.

## 0.9.2 (2017-12-13)

### Improvement
- [PR=87](https://github.com/salesforce/storm-dynamic-spout/pull/87) More fair partition distribution 

### Bug Fixes
- [ISSUE-92](https://github.com/salesforce/storm-dynamic-spout/issues/92) Fix race condition while cleaning up consumer state.
- [PR-86](https://github.com/salesforce/storm-dynamic-spout/pull/86) Sideline payload null checking
- [PR-88](https://github.com/salesforce/storm-dynamic-spout/pull/88) Sideline payload null checking

## 0.9.1 (2017-12-05)
### Bug Fixes
- [PR-85](https://github.com/salesforce/storm-dynamic-spout/pull/85) Fix ordering bug in DefaultRetryManager. Always retry the earliest messageId.

## 0.9.0 (2017-10-26)
### Improvements
- [PR-14](https://github.com/salesforce/storm-dynamic-spout/pull/14) Update Kafka dependencies to 0.11.0.1
- [PR-15](https://github.com/salesforce/storm-dynamic-spout/pull/15) Update Storm dependencies to 1.1.1
- [PR-24](https://github.com/salesforce/storm-dynamic-spout/pull/24) Add ability for errors to be reported up to the Storm web UI.
- [PR-34](https://github.com/salesforce/storm-dynamic-spout/pull/34) Add removeVirtualSpout() method to DynamicSpout
- [PR-35](https://github.com/salesforce/storm-dynamic-spout/pull/35/files) Output fields should now be declared as a List of String objects. In 0.10 we will drop the comma delimited strings, in the interim we are also now trimming whitespace off of the comma delimited version. 

### Bug Fixes
##### Kafka Consumer
- [PR-16](https://github.com/salesforce/storm-dynamic-spout/pull/16) Improved handling of Out Of Range exceptions
##### Sideline Spout
- [PR-13](https://github.com/salesforce/storm-dynamic-spout/pull/13) Fixed race condition on-redeploy of Spout when a 
sideline is active for firehose.

### Removed
##### Kafka Consumer
- [PR-31](https://github.com/salesforce/storm-dynamic-spout/pull/31) Removed configuration items: `spout.kafka.autocommit` and `spout.kafka.autocommit_interval_ms`.  This functionality is covered by configuration item `spout.cordinator.consumer_state_flush_interval_ms`

## 0.8.x
- Split the dynamic spout framework into `com.salesforce.storm.spout.dynamic`.
  - Anyone implementing a `DynamicSpout` will need to update package names.
- Checkstyle is enforced everywhere now.

## 0.7.x
- Simplified triggers, new singular and cleaner interface for implementing.

## 0.6.x
- MetricsRecorder is now a required dependency for VirtualSpout instances.

## 0.5.x
- Restructured config.

## 0.4.x
The goal of this release was to split off the sideline specific code from the dynamic spout framework such that it can be reused in other projects. 
- DynamicSpout is no longer an abstract class, now it can be attached directly to Storm as is.
- SidelineSpout does nothing other than change configuration, specifically setting the handler classes.
- SpoutHandler is a new way to define methods to tie into the DynamicSpout lifecycle.
- VirtualSpoutHandler is a new way to define methods to tie into the VirtualSpout lifecycle.
- New sidelining tests that are leaner and faster.
- Renamed SidelineSpoutConfig to SpoutConfig and changed the sideline_spout.* keys to spout.*, the sideline specific ones have been renamed to just sideline.*
- SpoutConfig uses a new annotation for config, and there's a new script to auto-generate the markdown table for it.

## 0.3.x
- Renamed and reorganized classes to separate kafka specific implementation details

## 0.2.x
- `SidelineRequest`'s now include the `SidelineIdentifier` which can now be explicitly supplied.  The original constructor that generated a UUID is now deprecated. 

## 0.1.x
- Initial release!
