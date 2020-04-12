package com.bigdata

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import java.time.LocalDateTime
import java.util.*


fun main(args: Array<String>) {
    KafkaConsumer("localhost:9092").process()
}


class KafkaConsumer(val brokers: String) {

    private val jsonMapper = ObjectMapper().apply {
        registerKotlinModule()
    }.registerModule(JavaTimeModule())

    fun process() {
        val streamsBuilder = StreamsBuilder()

        val tripJsonStream: KStream<String, String> = streamsBuilder
            .stream<String, String>("input-topic-2", Consumed.with(Serdes.String(), Serdes.String()))

        val tripStream: KStream<String, Trip> = tripJsonStream.mapValues { v ->
            jsonMapper.readValue(v, Trip::class.java)
        }

        val resStream: KStream<String, String> = tripStream
            .map { _, t ->
                KeyValue("${t.id} ${t.gender}", "${t.eventTime}")
            }

        resStream.to("output-topic", Produced.with(Serdes.String(), Serdes.String()))
        val topology = streamsBuilder.build()

        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["application.id"] = "kafka-tutorial"
        val streams = KafkaStreams(topology, props)
        streams.start()
    }

}

data class Trip(
    val id: Int,
    val start_stop: String,
    val eventTime: LocalDateTime,
    val stationId: Int,
    val duration: Double,
    val userType: String,
    val gender: String,
    val week: Int,
    val temperature: Double,
    val events: String
)