#!/bin/bash
path='/usr/local/kafka'

#sudo apt install xterm

cd $path

#bash -c "bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic input-topic --from-beginning" &
bash -c "bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic etl-topic --from-beginning" &
bash -c "bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic anomalies-topic --from-beginning"

#xterm -e
#xterm -e
#xterm -e