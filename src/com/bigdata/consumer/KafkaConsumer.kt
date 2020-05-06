package com.bigdata.consumer

import com.bigdata.lib.jsonMapper
import com.bigdata.model.ConsumerDateTimeKey
import com.bigdata.model.Station
import com.bigdata.model.Trip
import com.bigdata.model.TripStation
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.state.Stores
import java.io.File
import java.util.*

fun main(args: Array<String>) {
    args.forEach { println(it) }
    var P: Long = 50L
    var D: Long = 60L
    var file = "./src/com/bigdata/resources/Divvy_Bicycle_Stations.csv"
    if (args.size > 2) {
        file = args[0]
        P = args[1].toLong()
        D = args[2].toLong()
    }

    KafkaConsumer("localhost:9092").process(P, D, file)
}

class KafkaConsumer(private val brokers: String) {

    fun process(P: Long, D: Long, file: String) {
        val streamsBuilder = StreamsBuilder()

        val stations = getStations(file).toList()

        val tripJsonStream: KStream<String, String> = streamsBuilder
            .stream<String, String>("input-topic", Consumed.with(Serdes.String(), Serdes.String()))

        val tripStream: KStream<ConsumerDateTimeKey, TripStation> = tripJsonStream.map { _, v ->
            val trip = jsonMapper.readValue(v, Trip::class.java)
            val value = stations.firstOrNull { it.id == trip.stationId }?.let { TripStation(trip, it) }
                    ?: throw Exception("No such station")
            val key = ConsumerDateTimeKey(value)
            KeyValue(key, value)
        }

        // create store
        val storeBuilder =
            Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("state-store"), Serdes.String(), Serdes.String())
        //add store
        streamsBuilder.addStateStore(storeBuilder)

        tripStream
            .map { k, v -> KeyValue(k.toString(), v.toString()) }
            .transform({ CustomProcessor("state-store") }, arrayOf("state-store"))
            .foreach { k, v ->
                println("$k, value: $v")
            }

        val topology = streamsBuilder.build()

        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["application.id"] = "kafka-project"
        props["commit.interval.ms"] = 0
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
        props[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = EventTimeExtractor::class.java

        val streams = KafkaStreams(topology, props)
        streams.cleanUp()
        streams.start()
    }

    private fun getStations(file: String): Sequence<Station> =
        File(file)
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