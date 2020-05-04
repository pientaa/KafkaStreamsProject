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
    constructor(window: Window, tsCount: TripStationCount) : this(
        window = "Window(start=${LocalDateTime.ofInstant(window.startTime(), TimeZone.getDefault().toZoneId())}," +
                " end=${LocalDateTime.ofInstant(window.endTime(), TimeZone.getDefault().toZoneId())})",
        stationName = tsCount.stationName,
        returnedOverRented = if (tsCount.ended > tsCount.started) tsCount.ended - tsCount.started else 0,
        rentedOverReturned = if (tsCount.ended < tsCount.started) tsCount.started - tsCount.ended else 0,
        totalDocks = tsCount.totalDocks,
        nToDocksRatio = abs(tsCount.ended - tsCount.started).toDouble() / tsCount.totalDocks.toDouble()
    )

    override fun toString(): String {
        return jsonMapper.writeValueAsString(this)
    }
}