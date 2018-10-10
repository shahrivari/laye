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

    val UrlBatches = metricRegistry.meter("UrlBatches")
    val CheckUrl = metricRegistry.meter("CheckUrl")

    val TagBatches = metricRegistry.meter("TagBatches")
    val CheckTag = metricRegistry.meter("CheckTag")

    val UsrBatches = metricRegistry.meter("UsrBatches")
    val CheckUsr = metricRegistry.meter("CheckUsr")

    val UrlInIgnite = metricRegistry.meter("UrlInIgnite")
    val UrlNotInIgnite = metricRegistry.meter("UrlNotInIgnite")

    val TagInIgnite = metricRegistry.meter("TagInIgnite")
    val TagNotInIgnite = metricRegistry.meter("TagNotInIgnite")

    val UsrInIgnite = metricRegistry.meter("UsrInIgnite")
    val UsrNotInIgnite = metricRegistry.meter("UsrNotInIgnite")

    val UrlInRedis = metricRegistry.meter("UrlInRedis")
    val UrlNotInRedis = metricRegistry.meter("UrlNotInRedis")
    val UrlIn5min = metricRegistry.meter("UrlIn5min")

    val TagInRedis = metricRegistry.meter("TagInRedis")
    val TagNotInRedis = metricRegistry.meter("TagNotInRedis")
    val TagIn5min = metricRegistry.meter("TagIn5min")

    val UsrInRedis = metricRegistry.meter("UsrInRedis")
    val UsrNotInRedis = metricRegistry.meter("UsrNotInRedis")
    val UsrIn5min = metricRegistry.meter("UsrIn5min")
    val UsrBlocked = metricRegistry.meter("UsrBlocked")


    fun MarkUrlBatches(l: Long = 1) = UrlBatches.mark(l)
    fun MarkCheckUrl(l: Long = 1) = CheckUrl.mark(l)

    fun MarkTagBatches(l: Long = 1) = TagBatches.mark(l)
    fun MarkCheckTag(l: Long = 1) = CheckTag.mark(l)

    fun MarkUrlInIgnite(l: Long = 1) = UrlInIgnite.mark(l)
    fun MarkUrlNotInIgnite(l: Long = 1) = UrlNotInIgnite.mark(l)

    fun MarkTagInIgnite(l: Long = 1) = TagInIgnite.mark(l)
    fun MarkTagNotInIgnite(l: Long = 1) = TagNotInIgnite.mark(l)

    fun MarkUrlIn5min(l: Long = 1) = UrlIn5min.mark(l)
    fun MarkUsrIn5min(l: Long = 1) = UsrIn5min.mark(l)
    fun MarkTagIn5min(l: Long = 1) = TagIn5min.mark(l)

    fun MarkUsrBatches(l: Long = 1) = UsrBatches.mark(l)
    fun MarkCheckUsr(l: Long = 1) = CheckUsr.mark(l)

    fun MarkUsrInIgnite(l: Long = 1) = UsrInIgnite.mark(l)
    fun MarkUsrNotInIgnite(l: Long = 1) = UsrNotInIgnite.mark(l)
    fun MarkUsrBlocked(l: Long = 1) = UsrBlocked.mark(l)


    fun MarkInRedis(l: Long = 1, name: String) {
        if (name == "URL") UrlInRedis.mark(l)
        if (name == "TAG") TagInRedis.mark(l)
        if (name == "USR") UsrInRedis.mark(l)

    }

    fun MarkNotInRedis(l: Long = 1, name: String) {
        if (name == "URL") UrlNotInRedis.mark(l)
        if (name == "TAG") TagNotInRedis.mark(l)
        if (name == "USR") UsrNotInRedis.mark(l)

    }

    fun <T> addGauge(name: String, supplier: Supplier<T>) = metricRegistry.register(name, Gauge<T> { supplier.get() })

    private fun sortMetersByCount(meters: Map<String, Meter>) =
            meters.toList().sortedBy { it.second.count }.reversed()
                    .map { Pair(it.first, it.second.toPojo()) }.toMap()

    private fun Meter.toPojo() = MeterPojo(count, meanRate, oneMinuteRate, fiveMinuteRate, fifteenMinuteRate)

    fun getInfo() = ServerInfo(metricRegistry.gauges.mapValues { it.value.value },
            sortMetersByCount(metricRegistry.meters))


}

