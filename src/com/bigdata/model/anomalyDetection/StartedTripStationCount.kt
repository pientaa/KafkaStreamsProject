package com.bigdata.model.anomalyDetection

import com.bigdata.lib.jsonMapper
import com.bigdata.model.TripStation

data class StartedTripStationCount(
    val stationName: String = "",
    val started: Long = 0L,
    val totalDocks: Long = 0L
) {
    constructor(tripStation: TripStation) : this(
        stationName = tripStation.stationName,
        totalDocks = tripStation.stationTotalDocks
    )

    override fun toString(): String {
        return jsonMapper.writeValueAsString(this)
    }
}