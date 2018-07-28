package ir.rkr.cacheservice.util

import com.codahale.metrics.Gauge
import com.codahale.metrics.Meter
import com.codahale.metrics.MetricRegistry
import java.util.function.Supplier


data class MeterPojo(val count: Long,
                     val rate: Double,
                     val oneMinuteRate: Double,
                     val fiveMinuteRate: Double,
                     val fifteenMinuteRate: Double)

data class ServerInfo(val gauges: Map<String, Any>, val meters: Map<String, MeterPojo>)

class LayeMetrics {

    val metricRegistry = MetricRegistry()

    val CheckUrl = metricRegistry.meter("CheckUrl")
    val CheckTag = metricRegistry.meter("CheckTag")

    val UrlInIgnite = metricRegistry.meter("UrlInIgnite")
    val UrlNotInIgnite = metricRegistry.meter("UrlNotInIgnite")

    val TagInIgnite = metricRegistry.meter("TagInIgnite")
    val TagNotInIgnite = metricRegistry.meter("TagNotInIgnite")

    val UrlInRedis = metricRegistry.meter("UrlInRedis")
    val UrlNotInRedis = metricRegistry.meter("UrlNotInRedis")
    val UrlIn5min = metricRegistry.meter("UrlIn5min")

    val TagInRedis = metricRegistry.meter("TagInRedis")
    val TagNotInRedis = metricRegistry.meter("TagNotInRedis")
    val TagIn5min = metricRegistry.meter("TagIn5min")

    fun <T> addGauge(name: String, supplier: Supplier<T>) = metricRegistry.register(name, Gauge<T> { supplier.get() })

    fun MarkCheckUrl(l: Long = 1) = CheckUrl.mark(l)
    fun MarkCheckTag(l: Long = 1) = CheckTag.mark(l)

    fun MarkUrlInIgnite(l: Long = 1) = UrlInIgnite.mark(l)
    fun MarkUrlNotInIgnite(l: Long = 1) = UrlNotInIgnite.mark(l)

    fun MarkTagInIgnite(l: Long = 1) = TagInIgnite.mark(l)
    fun MarkTagNotInIgnite(l: Long = 1) = TagNotInIgnite.mark(l)

    fun MarkUrlIn5min(l: Long = 1) = UrlIn5min.mark(l)
    fun MarkTagIn5min(l: Long = 1) = TagIn5min.mark(l)

    fun MarkInRedis(l: Long = 1, name: String) {
        if (name == "URL") UrlInRedis.mark(l)
        if (name == "TAG") TagInRedis.mark(l)
    }

    fun MarkNotInRedis(l: Long = 1, name: String) {
        if (name == "URL") UrlNotInRedis.mark(l)
        if (name == "TAG") TagNotInRedis.mark(l)
    }

    private fun sortMetersByCount(meters: Map<String, Meter>) =
            meters.toList().sortedBy { it.second.count }.reversed()
                    .map { Pair(it.first, it.second.toPojo()) }.toMap()

    private fun Meter.toPojo() = MeterPojo(count, meanRate, oneMinuteRate, fiveMinuteRate, fifteenMinuteRate)

    fun getInfo() = ServerInfo(metricRegistry.gauges.mapValues { it.value.value },
            sortMetersByCount(metricRegistry.meters))


}

