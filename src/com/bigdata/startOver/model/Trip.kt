package com.bigdata.startOver.model

import com.bigdata.startOver.lib.jsonMapper
import java.time.LocalDateTime

data class Trip(
    val id: Int,
    val eventType: Int,  //start_stop – czy rozpoczęcie (0) czy zakończenie (1) przejazdu
    val eventTime: LocalDateTime,
    val stationId: Int,
    val duration: Double,
    val userType: String,
    val gender: String,
    val week: Int,
    val temperature: Double,
    val events: String
) {
    override fun toString(): String {
        return jsonMapper.writeValueAsString(this)
    }
}