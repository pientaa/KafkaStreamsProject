package com.bigdata.lib

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.time.LocalDateTime
import java.time.ZoneId

val jsonMapper: ObjectMapper = ObjectMapper().apply {
    registerKotlinModule()
}.registerModule(JavaTimeModule())!!

fun LocalDateTime.toMillis(zone: ZoneId = ZoneId.systemDefault()) = atZone(zone).toInstant().toEpochMilli()