package com.bigdata.consumer

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.processor.Processor
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.state.WindowStore

@Suppress("DEPRECATION")
class EndedTripsCountProcessor : Processor<String, String> {
    private var context: ProcessorContext? = null
    private var kvStore: WindowStore<Bytes, ByteArray>? = null

    override fun init(context: ProcessorContext) {
        // keep the processor context locally because we need it in punctuate() and commit()
        this.context = context

        // retrieve the key-value store named "Counts"
        kvStore = context.getStateStore("etl-store") as WindowStore<Bytes, ByteArray>

        // schedule a punctuate() method every 300_000 milliseconds based on stream-time
        this.context!!.schedule(300_000, PunctuationType.STREAM_TIME) {
            val iter = kvStore!!.all()
            while (iter.hasNext()) {
                val entry: KeyValue<Windowed<Bytes>, ByteArray>? = iter.next()
                if (entry != null) {
                    context.forward(entry.key, entry.value.toString())
                }
            }
            iter.close()

            // commit the current processing progress
            context.commit()
        }
    }

    override fun close() {
        // close any resources managed by this processor
        // Note: Do not close any StateStores as these are managed by the library
    }

    override fun process(p0: String?, p1: String?) {
        TODO("Not yet implemented")
    }
}