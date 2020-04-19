package com.bigdata.startOver.model

data class AggregatedInfo(
    val consumerDateKey: ConsumerDateKey,
    val avgTemperature: Double,
    val startedTrips: Long,
    val endedTrips: Long
)