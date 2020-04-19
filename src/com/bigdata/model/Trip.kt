package com.bigdata.model

import com.bigdata.lib.jsonMapper
import java.time.LocalDateTime

data class Trip(
    val id: Int,
    val type: Int,  //start_stop – czy rozpoczęcie (0) czy zakończenie (1) przejazdu
    val dateTime: LocalDateTime,
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