package com.bigdata.startOver.model

data class Station(
    val id: Int,
    val name: String,
    val totalDocks: Int,
    val docksInService: Int,
    val status: String,
    val latitude: Double,
    val longitude: Double,
    val location: String
)
