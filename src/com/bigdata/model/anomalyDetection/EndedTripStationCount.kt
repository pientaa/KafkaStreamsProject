package com.bigdata.model.anomalyDetection

import com.bigdata.lib.jsonMapper
import com.bigdata.model.TripStation

data class EndedTripStationCount(
    val stationName: String = "",
    val ended: Long = 0L,
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