package com.bigdata.consumer

import com.bigdata.lib.jsonMapper
import com.bigdata.model.*
import com.bigdata.model.anomalyDetection.EndedTripStationCount
import com.bigdata.model.anomalyDetection.StartedTripStationCount
import com.bigdata.model.anomalyDetection.TripStationSummaryInfo
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import java.io.File
import java.time.Duration
import java.util.*

fun main(args: Array<String>) {
    var P: Long = 50L
    var D: Long = 60L
    if (args.size > 1) {
        P = args[0].toLong()
        D = args[1].toLong()
    }

    KafkaConsumer("localhost:9092").process(P, D)
}

class KafkaConsumer(private val brokers: String) {

    fun process(P: Long, D: Long) {
        val streamsBuilder = StreamsBuilder()

        val stations = getStations().toList()

        val tripJsonStream: KStream<String, String> = streamsBuilder
            .stream<String, String>("input-topic", Consumed.with(Serdes.String(), Serdes.String()))

        val tripStream: KStream<ConsumerDateTimeKey, TripStation> = tripJsonStream.map { _, v ->
            val trip = jsonMapper.readValue(v, Trip::class.java)
            val value = stations.firstOrNull { it.id == trip.stationId }?.let { TripStation(trip, it) }
                    ?: throw Exception("No such station")
            val key = ConsumerDateTimeKey(value)
            KeyValue(key, value)
        }

        val dailyEndedTrips: KTable<Windowed<String>, Long> = tripStream
            .filter { _, v -> v.tripType == 0 }
            .map { k, v -> KeyValue(k, v.toString()) }
            .groupBy { consumerKey, _ -> jsonMapper.writeValueAsString(ConsumerDateKey(consumerKey)) }
            .windowedBy(TimeWindows.of(Duration.ofDays(1))/*.advanceBy(Duration.ofMinutes(5))*/.grace(Duration.ZERO))
            .count(/*Materialized.`as`<String, Long, WindowStore<Bytes, ByteArray>>("EndedTripsCountStore")*/)

        val dailyStartedTrips: KTable<Windowed<String>, Long> = tripStream
            .filter { _, v -> v.tripType == 1 }
            .map { k, v -> KeyValue(k, v.toString()) }
            .groupBy { consumerKey, _ -> jsonMapper.writeValueAsString(ConsumerDateKey(consumerKey)) }
            .windowedBy(TimeWindows.of(Duration.ofDays(1))/*.advanceBy(Duration.ofMinutes(5))*/.grace(Duration.ZERO))
            .count(/*Materialized.`as`<String, Long, WindowStore<Bytes, ByteArray>>("StartedTripsCountStore")*/)

        val temperatureCount = tripStream
            .map { k, v -> KeyValue(k, v.toString()) }
            .groupBy { consumerKey, _ -> jsonMapper.writeValueAsString(ConsumerDateKey(consumerKey)) }
            .windowedBy(TimeWindows.of(Duration.ofDays(1))/*.advanceBy(Duration.ofMinutes(5))*/.grace(Duration.ZERO))
            .count(/*Materialized.`as`<String, Long, WindowStore<Bytes, ByteArray>>("temperature-count-store")*/)

        val temperatureSum = tripStream
            .map { k, v -> KeyValue(k, v.temperature.toString()) }
            .groupBy { consumerKey, _ -> jsonMapper.writeValueAsString(ConsumerDateKey(consumerKey)) }
            .windowedBy(TimeWindows.of(Duration.ofDays(1))/*.advanceBy(Duration.ofMinutes(5))*/.grace(Duration.ZERO))
            .aggregate(
                { 0.0.toString() },
                { _, newV, aggV -> (aggV.toDouble() + newV.toDouble()).toString() }//,
//                Materialized.`as`<String, Double, WindowStore<Bytes, ByteArray>>("temperature-sum-store")
//                    .withValueSerde(Serdes.Double())
            )

        temperatureSum.join(temperatureCount) { sum, count -> sum.toDouble() / count.toDouble() }
            .join(dailyStartedTrips) { t, s -> Pair(t, s) }
            .join(dailyEndedTrips) { agg, e -> Triple(agg.first, agg.second, e) }
            .toStream()
            .map { k, v ->
                val key = jsonMapper.readValue(k.key(), ConsumerDateKey::class.java)
                KeyValue(k.key(), jsonMapper.writeValueAsString(AggregatedInfo(key, v.first, v.second, v.third)))
            }
            .groupByKey()
            .reduce { _, new -> new }
            .toStream()
            .through("etl-topic")
            .foreach { key, value ->
                println("Key: $key, value: $value")
            }

        //------------------------------------ anomalies --------------------------------------------------------------

        val endedTripPerHour: KTable<Windowed<String>, String> = tripStream
            .filter { _, v -> v.tripType == 0 }
            .map { k, v -> KeyValue(k, v.toString()) }
            .groupBy { consumerKey, _ -> jsonMapper.writeValueAsString(ConsumerDateKey(consumerKey)) }
            .windowedBy(TimeWindows.of(Duration.ofMinutes(D))/*.advanceBy(Duration.ofMinutes(5))*/.grace(Duration.ZERO))
            .aggregate(
                { EndedTripStationCount().toString() },
                { _, newV, aggV ->
                    val tripStation = jsonMapper.readValue(newV, TripStation::class.java)
                    val aggregate = jsonMapper.readValue(aggV, EndedTripStationCount::class.java)
                    EndedTripStationCount(tripStation).copy(ended = aggregate.ended + 1L).toString()
                }
            )

        val startedTripsPerHour: KTable<Windowed<String>, String> = tripStream
            .filter { _, v -> v.tripType == 1 }
            .map { k, v -> KeyValue(k, v.toString()) }
            .groupBy { consumerKey, _ -> jsonMapper.writeValueAsString(ConsumerDateKey(consumerKey)) }
            .windowedBy(TimeWindows.of(Duration.ofMinutes(D))/*.advanceBy(Duration.ofMinutes(5))*/.grace(Duration.ZERO))
            .aggregate(
                { StartedTripStationCount().toString() },
                { _, newV, aggV ->
                    val tripStation = jsonMapper.readValue(newV, TripStation::class.java)
                    val aggregate = jsonMapper.readValue(aggV, StartedTripStationCount::class.java)
                    StartedTripStationCount(tripStation).copy(started = aggregate.started + 1L).toString()
                }
            )

        startedTripsPerHour.join(endedTripPerHour) { started, ended -> Pair(started, ended) }
            .toStream()
            .map { k, v ->
                val pair = Pair(
                    jsonMapper.readValue(v.first, StartedTripStationCount::class.java),
                    jsonMapper.readValue(v.second, EndedTripStationCount::class.java)
                )
                KeyValue(k.key(), TripStationSummaryInfo(k.window(), pair))
            }
//                UNCOMMENT IF YOU WANT TO FILTER ONLY ANOMALIES
            .filter { _, v ->
                v.nToDocksRatio > P / 100.0
            }
            .map { k, v ->
                KeyValue(k, v.toString())
            }
            .groupByKey()
            .reduce { _, new -> new }
            .toStream()
            .through("anomalies-topic")
            .foreach { key, value ->
                println("Key: $key, value: $value")
            }

        val topology = streamsBuilder.build()

        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["application.id"] = "kafka-project"
        props["commit.interval.ms"] = 10000
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
        props[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = EventTimeExtractor::class.java

        val streams = KafkaStreams(topology, props)
        streams.start()
    }

    private fun getStations(): Sequence<Station> =
        File("./src/com/bigdata/resources/Divvy_Bicycle_Stations.csv")
            .let {
                sequence {
                    it.useLines { lines ->
                        lines.forEach { yield(it) }
                    }
                }
            }.filter { !it.contains("ID") }
            .map { line ->
                line.split(',').let {
                    Station(
                        it[0].toInt(), it[1], it[2].toLong(), it[3].toLong(),
                        it[4], it[5].toDouble(), it[6].toDouble(), it[7]
                    )
                }
            }
}