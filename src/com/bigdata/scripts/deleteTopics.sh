#!/bin/bash
path='/usr/local/kafka'

cd $path

bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic 'input.*'

bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic 'anomalies.*'

bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic 'etl.*'

bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic 'kafka.*'