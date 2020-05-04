#!/bin/bash
path='/usr/local/kafka'

sudo systemctl start zookeeper
sudo systemctl start kafka
sudo systemctl status kafka

cd $path

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic input-topic

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic etl-topic

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic anomalies-topic

bin/kafka-topics.sh --list --zookeeper localhost:2181