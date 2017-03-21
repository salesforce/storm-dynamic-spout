<a name="storm-sideline-spout"></a>
# Storm Sidelining Kafka Spout

Some random timings for future reference:

> ==== nextTuple() Totals after 75000000 calls ====
> nextTuple() totalCalls => 75000000
> nextTuple() totalTime => 317228 ms (100.0%)
> nextTuple() tupleMessageId => 3634 ms (1.1455482%)
> nextTuple() isFiltered => 3089 ms (0.97374755%)
> nextTuple() failedRetry => 10809 ms (3.4073286%)
> nextTuple() kafkaMessage => 3960 ms (1.2483134%)
> nextTuple() nextRecord => 118934 ms (37.491646%)
> nextTuple() doesExceedEndOffset => 2937 ms (0.9258325%)
> nextTuple() deserialize => 134894 ms (42.522728%)


> ==== ack() Totals after 75000000 calls ====
> ack() CommitOffset => 98977 ms (69.50144%)
> ack() TotalCalls => 75000000 ms (52664.844%)
> ack() UpdateMetrics => 0 ms (0.0%)
> ack() RemoveTracked => 13305 ms (9.342743%)
> ack() TotalTime => 142410 ms (100.0%)
> ack() TupleMessageId => 2919 ms (2.0497155%)
> ack() FailedMsgAck => 7675 ms (5.389369%)


## Purpose of this project
The purpose of this project is to provide a ....

## What is NOT the purpose of this project
....  
 
## So what does this Spout do?

# Getting started
## Configuration

# Metrics

# Customization
## Interfaces
### Interface A
### Interface B
### Interface C



 
