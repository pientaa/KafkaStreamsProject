package com.bigdata.model

import com.bigdata.lib.jsonMapper
import java.time.LocalDateTime

data class ConsumerDateTimeKey(
    val stationName: String,
    val eventTime: LocalDateTime
) {
    constructor(tripStation: TripStation) : this(
        stationName = tripStation.stationName,
        eventTime = tripStation.tripTime
    )

    override fun toString(): String {
        return jsonMapper.writeValueAsString(this)
    }
}