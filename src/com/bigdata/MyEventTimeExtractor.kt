package com.bigdata

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor
import java.time.LocalDateTime
import java.time.ZoneId

class MyEventTimeExtractor : TimestampExtractor {
    override fun extract(
        record: ConsumerRecord<Any, Any>,
        previousTimestamp: Long
    ): Long {
        var timestamp: Long = -1
        val stringLine: String
        if (record.value() is String) {
            stringLine = record.value() as String
            timestamp = jsonMapper.readValue(stringLine, Trip::class.java).eventTime.toMillis()
        }
        return timestamp
    }

    private val jsonMapper = ObjectMapper().apply {
        registerKotlinModule()
    }.registerModule(JavaTimeModule())
}

fun LocalDateTime.toMillis(zone: ZoneId = ZoneId.systemDefault()) = atZone(zone).toInstant().toEpochMilli()