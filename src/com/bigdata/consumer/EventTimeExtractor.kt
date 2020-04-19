package com.bigdata.consumer

import com.bigdata.lib.toMillis
import com.bigdata.model.ConsumerDateKey
import com.bigdata.model.ConsumerDateTimeKey
import com.bigdata.model.Trip
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor
import java.time.LocalDateTime
import java.time.LocalTime

class EventTimeExtractor : TimestampExtractor {
    override fun extract(
        record: ConsumerRecord<Any, Any>,
        previousTimestamp: Long
    ): Long {
        var timestamp: Long = -1
        val value: String
        if (record.value() is String) {
            value = record.value() as String
            timestamp = when (record.key()) {
                is ConsumerDateKey -> jsonMapper.readValue(value, Trip::class.java).dateTime.toMillis()
                is ConsumerDateTimeKey -> {
                    LocalDateTime.of(
                        jsonMapper.readValue(record.key() as String, ConsumerDateKey::class.java).eventDay,
                        LocalTime.MIN
                    ).toMillis()
                }
                else ->
                    jsonMapper.readValue(value, Trip::class.java).dateTime.toMillis()
            }
        }
        return timestamp
    }

    private val jsonMapper = ObjectMapper().apply {
        registerKotlinModule()
    }.registerModule(JavaTimeModule())
}