#!/bin/bash
set -e

## Build package
mvn package -DskipTests=true

## Build DynamicSpout docs
java -cp target/*jar-with-dependencies.jar com.salesforce.storm.spout.dynamic.config.DocTask

## Build KafkaConsumer docs
java -cp target/*jar-with-dependencies.jar com.salesforce.storm.spout.dynamic.kafka.DocTask

## Build Sideline docs
java -cp target/*jar-with-dependencies.jar com.salesforce.storm.spout.sideline.config.DocTask
