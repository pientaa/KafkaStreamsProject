package com.bigdata.model.anomalyDetection

import com.bigdata.lib.jsonMapper
import org.apache.kafka.streams.kstream.Window
import java.time.LocalDateTime
import java.util.*
import kotlin.math.abs

data class TripStationSummaryInfo(
    val window: String,
    val stationName: String,
    val returnedOverRented: Long,
    val rentedOverReturned: Long,
    val totalDocks: Long,
    val nToDocksRatio: Double
) {
    constructor(window: Window, tsCount: Pair<StartedTripStationCount, EndedTripStationCount>) : this(
        window = "Window(start=${LocalDateTime.ofInstant(window.startTime(), TimeZone.getDefault().toZoneId())}," +
                " end=${LocalDateTime.ofInstant(window.endTime(), TimeZone.getDefault().toZoneId())})",
        stationName = tsCount.first.stationName,
        returnedOverRented = if (tsCount.second.ended > tsCount.first.started) tsCount.second.ended - tsCount.first.started else 0,
        rentedOverReturned = if (tsCount.second.ended < tsCount.first.started) tsCount.first.started - tsCount.second.ended else 0,
        totalDocks = tsCount.first.totalDocks,
        nToDocksRatio = abs(tsCount.second.ended - tsCount.first.started).toDouble() / tsCount.first.totalDocks.toDouble()
    )

    override fun toString(): String {
        return jsonMapper.writeValueAsString(this)
    }
}