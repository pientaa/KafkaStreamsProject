package com.bigdata.model

import com.bigdata.lib.jsonMapper
import java.time.LocalDate

data class ConsumerDateKey(
    val stationName: String = "",
    val eventDay: LocalDate = LocalDate.now()
) {
    constructor(consumerDateTimeKey: ConsumerDateTimeKey) : this(
        stationName = consumerDateTimeKey.stationName,
        eventDay = consumerDateTimeKey.eventTime.toLocalDate()
    )

    override fun toString(): String {
        return jsonMapper.writeValueAsString(this)
    }
}