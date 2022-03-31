#!/bin/bash

$KAFKA_HOME/bin/kafka-topics.sh --create --topic simple --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1