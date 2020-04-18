package com.bigdata.startOver

import com.bigdata.Trip
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
import org.apache.kafka.streams.state.WindowStore
import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

fun main(args: Array<String>) {
    KafkaSecondConsumer("localhost:9092").process()
}

class KafkaSecondConsumer(private val brokers: String) {

    private val jsonMapper = ObjectMapper().apply {
        registerKotlinModule()
    }.registerModule(JavaTimeModule())

    fun process() {
        val streamsBuilder = StreamsBuilder()

        val tripJsonStream: KStream<String, String> = streamsBuilder
            .stream<String, String>("input-topic", Consumed.with(Serdes.String(), Serdes.String()))

        val tripStream: KStream<ConsumerDateTimeKey, Trip> = tripJsonStream.map { _, v ->
            val value = jsonMapper.readValue(v, Trip::class.java)
            val key = ConsumerDateTimeKey(value)
            println("Key: $key, value: $value")
            KeyValue(key, value)
        }

        val endedTrips: KTable<Windowed<String>, Long> = tripStream
            .filter { _, v -> v.eventType == 0 }
            .map { k, v -> KeyValue(k, v.toString()) }
            .groupBy { consumerKey, _ -> jsonMapper.writeValueAsString(
                ConsumerDateKey(
                    consumerKey
                )
            ) }
            .windowedBy(TimeWindows.of(Duration.ofDays(1))/*.advanceBy(Duration.ofMinutes(5))*/)
            .count(/*Materialized.`as`<String, Long, WindowStore<Bytes, ByteArray>>("EndedTripsCountStore")*/)

        val startedTrips: KTable<Windowed<String>, Long> = tripStream
            .filter { _, v -> v.eventType == 1 }
            .map { k, v -> KeyValue(k, v.toString()) }
            .groupBy { consumerKey, _ -> jsonMapper.writeValueAsString(
                ConsumerDateKey(
                    consumerKey
                )
            ) }
            .windowedBy(TimeWindows.of(Duration.ofDays(1))/*.advanceBy(Duration.ofMinutes(5))*/)
            .count(/*Materialized.`as`<String, Long, WindowStore<Bytes, ByteArray>>("StartedTripsCountStore")*/)

        val temperatureCount = tripStream
            .map { k, v -> KeyValue(k, v.toString()) }
            .groupBy { consumerKey, _ -> jsonMapper.writeValueAsString(
                ConsumerDateKey(
                    consumerKey
                )
            ) }
            .windowedBy(TimeWindows.of(Duration.ofDays(1)).grace(Duration.ZERO))
            .count(Materialized.`as`<String, Long, WindowStore<Bytes, ByteArray>>("temperature-count-store"))

        val temperatureSum = tripStream
            .map { k, v -> KeyValue(k, v.temperature.toString()) }
            .groupBy { consumerKey, _ -> jsonMapper.writeValueAsString(
                ConsumerDateKey(
                    consumerKey
                )
            ) }
            .windowedBy(TimeWindows.of(Duration.ofDays(1)))
            .aggregate(
                { 0.0 },
                { _, newV, aggV -> aggV + newV.toDouble() },
                Materialized.`as`<String, Double, WindowStore<Bytes, ByteArray>>("temperature-sum-store")
                    .withValueSerde(Serdes.Double())
            )

        val startedEnded = startedTrips.join(endedTrips) { started: Long, ended: Long -> Pair(started, ended) }
            .toStream()
            .map { k, v -> KeyValue(k, v.toString()) }
        startedEnded
            .foreach { key, value ->
                println("Key: $key, ended, started: $value")
            }

        temperatureSum.join(temperatureCount) { sum, count -> sum / count.toDouble() }
            .toStream()
            .map { k, v -> KeyValue(k, v.toString()) }
            .foreach { key, value ->
                println("Key: $key, value: $value")
            }
        temperatureSum.join(temperatureCount) { sum, count -> sum / count.toDouble() }
            .join(startedTrips) { t, s -> Pair(t, s) }
            .join(endedTrips) { agg, e -> Triple(agg.first, agg.second, e) }
            .toStream()
            .map { k, v ->
                val key = jsonMapper.readValue(k.key(), ConsumerDateKey::class.java)
                KeyValue(k.key(), jsonMapper.writeValueAsString(
                    AggregatedInfo(
                        key,
                        v.first,
                        v.second,
                        v.third
                    )
                ))
            }
            .foreach { key, value ->
                println("Key: $key, value: $value")
            }

        val topology = streamsBuilder.build()

        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["application.id"] = "kafka-tutorial"
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
        props[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = EventTimeExtractor::class.java

        val streams = KafkaStreams(topology, props)
        streams.start()
    }
}

data class AggregatedInfo(
    val consumerDateKey: ConsumerDateKey,
    val avgTemperature: Double,
    val startedTrips: Long,
    val endedTrips: Long
)

data class ConsumerDateTimeKey(
    val stationId: Int,
    val eventTime: LocalDateTime
) {
    constructor(trip: Trip) : this(
        stationId = trip.stationId,
        eventTime = trip.eventTime
    )
}

data class ConsumerDateKey(
    val stationId: Int,
    val eventDay: LocalDate
) {
    constructor(trip: Trip) : this(
        stationId = trip.stationId,
        eventDay = trip.eventTime.toLocalDate()
    )

    constructor(consumerDateTimeKey: ConsumerDateTimeKey) : this(
        stationId = consumerDateTimeKey.stationId,
        eventDay = consumerDateTimeKey.eventTime.toLocalDate()
    )
}