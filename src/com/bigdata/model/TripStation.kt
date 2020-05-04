package com.bigdata.model

import com.bigdata.lib.jsonMapper
import java.time.LocalDateTime

data class TripStation(
    val tripId: Int,
    val tripType: Int,  //start or stop
    val tripTime: LocalDateTime,
    val stationId: Int,
    val duration: Double,
    val userType: String,
    val gender: String,
    val week: Int,
    val temperature: Double,
    val events: String,
    val stationName: String,
    val stationTotalDocks: Long,
    val stationDocksInService: Long,
    val stationStatus: String,
    val stationLatitude: Double,
    val stationLongitude: Double,
    val stationLocation: String
) {
    constructor(trip: Trip, station: Station) : this(
        tripId = trip.id,
        tripType = trip.type,
        tripTime = trip.dateTime,
        stationId = station.id,
        duration = trip.duration,
        userType = trip.userType,
        gender = trip.gender,
        week = trip.week,
        temperature = trip.temperature,
        events = trip.events,
        stationName = station.name,
        stationTotalDocks = station.totalDocks,
        stationDocksInService = station.docksInService,
        stationStatus = station.status,
        stationLatitude = station.latitude,
        stationLongitude = station.longitude,
        stationLocation = station.location
    )

    override fun toString(): String {
        return jsonMapper.writeValueAsString(this)
    }
}