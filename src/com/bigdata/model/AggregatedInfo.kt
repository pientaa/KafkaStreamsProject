package com.bigdata.model

import com.bigdata.lib.jsonMapper

data class AggregatedInfo(
    val consumerDateKey: ConsumerDateKey? = ConsumerDateKey(),
    val avgTemperature: Double = 0.0,
    val startedTrips: Long = 0L,
    val endedTrips: Long = 0L
) {
    override fun toString(): String {
        return jsonMapper.writeValueAsString(this)
    }
}