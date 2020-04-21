#!/bin/bash
FILE=${1}
P=${2}
D=${3}
path='../../../../out/artifacts/KafkaStreamsProject_jar/'
#java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar com.bigdata.producer.KafkaProducerKt

cd $path

java -cp KafkaStreamsProject.jar com.bigdata.consumer.KafkaConsumerKt ${FILE} ${P} ${D}

#./processing.sh ./../../../src/com/bigdata/resources/Divvy_Bicycle_Stations.csv 10 10
