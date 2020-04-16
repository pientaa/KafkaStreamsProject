package com.bigdata

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.WindowStore
import java.io.File
import java.time.Duration
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.TimeUnit

fun main(args: Array<String>) {
    KafkaConsumer("localhost:9092").process()
}

class KafkaConsumer(private val brokers: String) {

    private val jsonMapper = ObjectMapper().apply {
        registerKotlinModule()
    }.registerModule(JavaTimeModule())

    fun process() {
        val streamsBuilder = StreamsBuilder()

        val tripJsonStream: KStream<String, String> = streamsBuilder
            .stream<String, String>("input-topic", Consumed.with(Serdes.String(), Serdes.String()))

        val tripStream: KStream<String, Trip> = tripJsonStream.mapValues { v ->
            jsonMapper.readValue(v, Trip::class.java)
        }

        val trips = tripStream
            .map { _, v -> KeyValue("${v.stationId} ${v.eventTime.toLocalDate()}", jsonMapper.writeValueAsString(v)) }

        val startTrips = trips
            .filter { _, v -> jsonMapper.readValue(v, Trip::class.java).eventType == 0 }
            .map { k, v -> KeyValue("$k start", v) }
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofHours(24)).advanceBy(Duration.ofMinutes(5)))
            .count(Materialized.`as`<String, Long, WindowStore<Bytes, ByteArray>>("startTripsStore-8"))

        val endTrips = trips
            .filter { _, v -> jsonMapper.readValue(v, Trip::class.java).eventType == 1 }
            .map { k, v -> KeyValue("$k end", v) }
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofHours(24)).advanceBy(Duration.ofMinutes(5)))
            .count(Materialized.`as`<String, Long, WindowStore<Bytes, ByteArray>>("endTripsStore-8"))

        startTrips.toStream().merge(endTrips.toStream())
            .map { k, v -> KeyValue(k.key(), v.toString()) }
            .groupByKey()
            .reduce { _, new -> new }
            .toStream()
            .map { k, v ->
                when {
                    k.contains("end") -> KeyValue(k.replace(" end", ""), v.plus(" ended"))
                    k.contains("start") -> KeyValue(k.replace(" start", ""), v.plus(" started"))
                    else -> KeyValue(k, v)
                }
            }
            .map { k, v -> KeyValue(k, "Key: $k, Value: $v") }

            .to("output-topic", Produced.with(Serdes.String(), Serdes.String()))

        val topology = streamsBuilder.build()

        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["application.id"] = "kafka-tutorial"
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
        props[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = MyEventTimeExtractor::class.java

        val streams = KafkaStreams(topology, props)
        streams.start()
    }

    fun produce(): Sequence<Station> =
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
                        it[0].toInt(), it[1], it[2].toInt(), it[3].toInt(),
                        it[4], it[5].toDouble(), it[6].toDouble(), it[7]
                    )
                }
            }
}

data class Station(
    val id: Int,
    val name: String,
    val totalDocks: Int,
    val docksInService: Int,
    val status: String,
    val latitude: Double,
    val longitude: Double,
    val location: String
)

data class Trip(
    val id: Int,
    val eventType: Int,  //start_stop – czy rozpoczęcie (0) czy zakończenie (1) przejazdu
    val eventTime: LocalDateTime,
    val stationId: Int,
    val duration: Double,
    val userType: String,
    val gender: String,
    val week: Int,
    val temperature: Double,
    val events: String
)