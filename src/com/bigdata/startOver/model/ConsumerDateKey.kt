package com.bigdata.startOver.model

import java.time.LocalDate

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