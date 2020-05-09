package com.bigdata.consumer

import com.bigdata.lib.jsonMapper
import com.bigdata.model.AggregatedInfo
import com.bigdata.model.ConsumerDateKey
import com.bigdata.model.ConsumerDateTimeKey
import com.bigdata.model.TripStation
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.state.KeyValueStore
import java.time.LocalDate

class CustomTransformer(
    private val stateStoreName: String
) : Transformer<String, String, KeyValue<String, String>> {

    private var context: ProcessorContext? = null
    private var kvStore: KeyValueStore<String, String>? = null

    override fun transform(key: String, value: String): KeyValue<String, String>? {
        val tripStation = jsonMapper.readValue(value, TripStation::class.java)

        // transform key to Date instead of DateTime
        val kvStoreKey = ConsumerDateKey(jsonMapper.readValue(key, ConsumerDateTimeKey::class.java))

        val aggr: String? = kvStore?.get(kvStoreKey.toString())
        val existingValue = aggr?.let { jsonMapper.readValue(it, AggregatedInfo::class.java) }

        val updatedValue =
            (existingValue ?: AggregatedInfo(consumerDateKey = kvStoreKey, updateTime = tripStation.tripTime)).let {
                when (tripStation.tripType) {
                    1 -> it.copy(startedTrips = it.startedTrips + 1L)
                    else -> it.copy(endedTrips = it.endedTrips + 1L)
                }
                    .copy(
                        avgTemperature = ((it.endedTrips + it.startedTrips) * it.avgTemperature + tripStation.temperature) /
                                (it.endedTrips + it.startedTrips + 1),
                        updateTime = tripStation.tripTime
                    )
            }

        kvStore?.put(kvStoreKey.toString(), updatedValue.toString())
        return null
    }

    override fun init(context: ProcessorContext) {
        this.context = context
        this.kvStore = context.getStateStore(stateStoreName) as KeyValueStore<String, String>

        var lastDate: LocalDate?

        val entryDateList: MutableList<LocalDate> = mutableListOf()

        this.context!!.schedule(300_000, PunctuationType.STREAM_TIME) {
            var iter = kvStore!!.all()
            while (iter.hasNext()) {
                val entry: KeyValue<String, String>? = iter.next()
                if (entry != null) {
                    entryDateList.add(jsonMapper.readValue(entry.key, ConsumerDateKey::class.java).eventDay)
                    context.forward(entry.key, entry.value.toString())
                }
            }
            iter.close()

            // commit the current processing progress
            context.commit()

            // delete these values that are already committed and from previous days
            lastDate = entryDateList.maxBy { it }
            iter = kvStore!!.all()
            while (iter.hasNext()) {
                val entry: KeyValue<String, String>? = iter.next()
                if (entry != null) {
                    val entryDate = jsonMapper.readValue(entry.key, ConsumerDateKey::class.java).eventDay
                    if (entryDate.isBefore(lastDate))
                        kvStore!!.delete(entry.key)
                }
            }

        }
    }

    override fun close() {
        TODO("Not yet implemented")
    }
}