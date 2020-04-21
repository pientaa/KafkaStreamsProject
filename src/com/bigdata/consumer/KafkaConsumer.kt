package com.bigdata.consumer

import com.bigdata.lib.jsonMapper
import com.bigdata.model.*
import com.bigdata.model.anomalyDetection.TripStationCount
import com.bigdata.model.anomalyDetection.TripStationSummaryInfo
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded
import org.apache.kafka.streams.state.WindowStore
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

        val foo = tripStream
            .map { k, v -> KeyValue(k, v.toString()) }
            .groupBy { consumerKey, _ -> jsonMapper.writeValueAsString(ConsumerDateKey(consumerKey)) }
            .windowedBy(
                TimeWindows.of(Duration.ofDays(1)).advanceBy(Duration.ofMinutes(5)).grace(Duration.ofMillis(0))
            )
            .aggregate(
                { AggregatedInfo().toString() },
                { aggK, newV, aggV ->
                    val key = jsonMapper.readValue(aggK, ConsumerDateKey::class.java)
                    val tripStation = jsonMapper.readValue(newV, TripStation::class.java)
                    val aggregated = jsonMapper.readValue(aggV, AggregatedInfo::class.java)

                    tripStation.let {
                        when (tripStation.tripType) {
                            1 -> aggregated.copy(consumerDateKey = key, startedTrips = aggregated.startedTrips + 1L)
                            else -> aggregated.copy(consumerDateKey = key, endedTrips = aggregated.endedTrips + 1L)
                        }
                    }.copy(
                        avgTemperature = ((aggregated.endedTrips + aggregated.startedTrips) * aggregated.avgTemperature + tripStation.temperature) /
                                (aggregated.endedTrips + aggregated.startedTrips + 1)
                    ).toString()
                },
                Materialized.`as`<String, String, WindowStore<Bytes, ByteArray>>("etl-store")
                    .withCachingDisabled().withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
            .suppress(Suppressed.untilWindowCloses(unbounded()))

        foo.toStream()
            .foreach { k, v ->
                println(v)
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
//                println("Key: $key, value: $value")
            }

        //------------------------------------ anomalies --------------------------------------------------------------
//
//        val endedTripPerHour: KTable<Windowed<String>, String> = tripStream
//            .filter { _, v -> v.tripType == 0 }
//            .map { k, v -> KeyValue(k, v.toString()) }
//            .groupBy { consumerKey, _ -> jsonMapper.writeValueAsString(ConsumerDateKey(consumerKey)) }
//            .windowedBy(TimeWindows.of(Duration.ofMinutes(D)).advanceBy(Duration.ofMinutes(5)).grace(Duration.ZERO))
//            .aggregate(
//                { EndedTripStationCount().toString() },
//                { _, newV, aggV ->
//                    val tripStation = jsonMapper.readValue(newV, TripStation::class.java)
//                    val aggregate = jsonMapper.readValue(aggV, EndedTripStationCount::class.java)
//                    EndedTripStationCount(tripStation).copy(ended = aggregate.ended + 1L).toString()
//                }
//            )
//            .suppress(Suppressed.untilWindowCloses(unbounded()))
//
//        val startedTripsPerHour: KTable<Windowed<String>, String> = tripStream
//            .filter { _, v -> v.tripType == 1 }
//            .map { k, v -> KeyValue(k, v.toString()) }
//            .groupBy { consumerKey, _ -> jsonMapper.writeValueAsString(ConsumerDateKey(consumerKey)) }
//            .windowedBy(TimeWindows.of(Duration.ofMinutes(D)).advanceBy(Duration.ofMinutes(5)).grace(Duration.ZERO))
//            .aggregate(
//                { StartedTripStationCount().toString() },
//                { _, newV, aggV ->
//                    val tripStation = jsonMapper.readValue(newV, TripStation::class.java)
//                    val aggregate = jsonMapper.readValue(aggV, StartedTripStationCount::class.java)
//                    StartedTripStationCount(tripStation).copy(started = aggregate.started + 1L).toString()
//                }
//            )
//            .suppress(Suppressed.untilWindowCloses(unbounded()))

        val anomalies: KTable<Windowed<String>, String> = tripStream
            .map { k, v -> KeyValue(k, v.toString()) }
            .groupBy { consumerKey, _ -> jsonMapper.writeValueAsString(ConsumerDateKey(consumerKey)) }
            .windowedBy(
                TimeWindows.of(Duration.ofMinutes(D)).advanceBy(Duration.ofMinutes(5)).grace(Duration.ofMillis(0))
            )
            .aggregate(
                { TripStationCount().toString() },
                { _, newV, aggV ->
                    val tripStation = jsonMapper.readValue(newV, TripStation::class.java)
                    val aggregated = jsonMapper.readValue(aggV, TripStationCount()::class.java)
                    when (tripStation.tripType) {
                        0 -> TripStationCount(tripStation).copy(ended = aggregated.ended + 1L).toString()
                        1 -> TripStationCount(tripStation).copy(started = aggregated.started + 1L).toString()
                        else -> aggregated.toString()
                    }
                },
                Materialized.`as`<String, String, WindowStore<Bytes, ByteArray>>("anomalies-store")
                    .withCachingDisabled().withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
            .suppress(Suppressed.untilWindowCloses(unbounded()))

        anomalies
            .toStream()
            .filter { k, v ->
                val tripStationCount = jsonMapper.readValue(v, TripStationCount::class.java)
                val value = TripStationSummaryInfo(k.window(), tripStationCount)
                value.nToDocksRatio > P / 100.0

                true
            }
            .map { k, v ->
                val tripStationCount = jsonMapper.readValue(v, TripStationCount::class.java)
                val value = TripStationSummaryInfo(k.window(), tripStationCount)
//                println("$value")
                KeyValue(k.key(), value.toString())
            }
            .to("anomalies-topic")
//
//        startedTripsPerHour.join(endedTripPerHour) { started, ended -> Pair(started, ended) }
//            .toStream()
//            .map { k, v ->
//                val pair = Pair(
//                    jsonMapper.readValue(v.first, StartedTripStationCount::class.java),
//                    jsonMapper.readValue(v.second, EndedTripStationCount::class.java)
//                )
//                KeyValue(k.key(), TripStationSummaryInfo(k.window(), pair))
//            }
//                UNCOMMENT IF YOU WANT TO FILTER ONLY ANOMALIES
//            .filter { _, v ->
//                v.nToDocksRatio > P / 100.0
//            }
//            .foreach { key, value ->
//                println("Window: $key, value: $value")
//            }

//            .map { k, v ->
//                KeyValue(k, v.toString())
//            }

//            .groupByKey()
//            .reduce { _, new -> new }
//            .toStream()
//            .through("anomalies-topic")
//            .foreach { key, value ->
//                println("Key: $key, value: $value")
//            }

        val topology = streamsBuilder.build()

        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["application.id"] = "kafka-project"
        props["commit.interval.ms"] = 0
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