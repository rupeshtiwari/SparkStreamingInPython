#!/bin/bash

$KAFKA_HOME/bin/kafka-topics.sh --create --topic invoices --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1