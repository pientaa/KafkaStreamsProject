#!/bin/bash
FOLDER=${1}
path='../../../../out/artifacts/KafkaStreamsProject_jar/'
#java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar com.bigdata.producer.KafkaProducerKt

cd $path

java -cp KafkaStreamsProject.jar com.bigdata.producer.KafkaProducerKt ${FOLDER}

#./producer.sh ./../../../src/com/bigdata/resources/producer
