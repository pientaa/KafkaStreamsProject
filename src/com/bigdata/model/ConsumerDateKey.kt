package com.bigdata.model

import java.time.LocalDate

data class ConsumerDateKey(
    val stationName: String,
    val eventDay: LocalDate
) {
    constructor(consumerDateTimeKey: ConsumerDateTimeKey) : this(
        stationName = consumerDateTimeKey.stationName,
        eventDay = consumerDateTimeKey.eventTime.toLocalDate()
    )
}