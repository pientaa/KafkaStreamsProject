package com.bigdata.model

data class Station(
    val id: Int,
    val name: String,
    val totalDocks: Long,
    val docksInService: Long,
    val status: String,
    val latitude: Double,
    val longitude: Double,
    val location: String
)
