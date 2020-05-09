# KafkaStreamsProject
## Prerequisites
Kafka broker on `localhost:9092` with topic `input-topic`

## Introduction
This repository consist of `KafkaProducer` and `KafkaConsumer`. We produce events of rents and returns of rented bikes to the particular stations. This is simulated with interval of 0.5 second (measured in real time) between next two events.

Then `KafkaConsumer` perform following aggregation.

### Aggregation
The objective was to create aggregation of streamed data and join this data with static description (loaded from csv).

Data was needed to be aggregated by day and aggregation was meant to be updated every 5 minutes (measured by streamed data timestamps).

We count both started and ended trips, and calculate average temparature each day.
