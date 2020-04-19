package com.bigdata.model

import java.time.LocalDate

data class ConsumerDateKey(
    val stationId: Int,
    val eventDay: LocalDate
) {
    constructor(consumerDateTimeKey: ConsumerDateTimeKey) : this(
        stationId = consumerDateTimeKey.stationId,
        eventDay = consumerDateTimeKey.eventTime.toLocalDate()
    )
}