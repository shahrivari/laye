package ir.rkr.cacheservice.util

import com.codahale.metrics.Meter
import com.codahale.metrics.MetricRegistry


data class MeterPojo(val count: Long,
                     val rate: Double,
                     val oneMinuteRate: Double,
                     val fiveMinuteRate: Double,
                     val fifteenMinuteRate: Double)


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

    val TagInRedis = metricRegistry.meter("TagInRedis")
    val TagNotInRedis = metricRegistry.meter("TagNotInRedis")

    fun MarkCheckUrl(l: Long = 1) = CheckUrl.mark(l)
    fun MarkCheckTag(l: Long = 1) = CheckTag.mark(l)

    fun MarkUrlInIgnite(l: Long = 1) = UrlInIgnite.mark(l)
    fun MarkUrlNotInIgnite(l: Long = 1) = UrlNotInIgnite.mark(l)

    fun MarkTagInIgnite(l: Long = 1) = TagInIgnite.mark(l)
    fun MarkTagNotInIgnite(l: Long = 1) = TagNotInIgnite.mark(l)


    fun MarkInRedis(l: Long = 1, name: String) {
        if (name == "URL") UrlInRedis.mark(l)
        if (name == "TAG") TagInRedis.mark(l)
    }

    fun MarkNotInRedis(l: Long = 1, name: String) {
        if (name == "URL") UrlNotInRedis.mark(l)
        if (name == "TAG") TagNotInRedis.mark(l)
    }

    private fun sortMetersByCount(meters: Map<String, Meter>) =
            meters.toList().sortedBy { it.second.count }.reversed().map { Pair(it.first, it.second.toPojo()) }.toMap()

    private fun Meter.toPojo() = MeterPojo(count, meanRate, oneMinuteRate, fiveMinuteRate, fifteenMinuteRate)

    fun getInfo() = sortMetersByCount(metricRegistry.meters)


}

