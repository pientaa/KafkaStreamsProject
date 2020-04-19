package com.bigdata.model

import java.time.LocalDateTime

data class ConsumerDateTimeKey(
    val stationId: Int,
    val eventTime: LocalDateTime
) {
    constructor(trip: Trip) : this(
        stationId = trip.stationId,
        eventTime = trip.dateTime
    )
}