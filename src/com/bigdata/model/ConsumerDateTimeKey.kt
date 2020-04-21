package com.bigdata.model

import java.time.LocalDateTime

data class ConsumerDateTimeKey(
    val stationName: String,
    val eventTime: LocalDateTime
) {
    constructor(tripStation: TripStation) : this(
        stationName = tripStation.stationName,
        eventTime = tripStation.tripTime
    )
}