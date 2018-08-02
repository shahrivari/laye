package ir.rkr.cacheservice.ignite

import ir.rkr.cacheservice.redis.RedisConnector
import ir.rkr.cacheservice.util.LayeMetrics
import mu.KotlinLogging
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.function.Supplier
import kotlin.concurrent.thread


internal class IgniteFeeder(ignite: IgniteConnector, redis: RedisConnector, queueSize: Int = 100_000,
                            val layemetrics: LayeMetrics, metricType: String) {

    private val logger = KotlinLogging.logger {}
    var tmpQueue: BlockingQueue<String> = ArrayBlockingQueue(queueSize)

    init {
        layemetrics.addGauge("${metricType}QueueSize", Supplier { tmpQueue.size })

        thread {
            while (true) {

                val keys = mutableListOf<String>()
                for (i in 1..50) {
                    val key = tmpQueue.poll()
                    if (key != null) keys.add(key)
                }


                if (keys.isNotEmpty()) {
                    val result = redis.mget(keys)

                    val availableResult = result.filterValues { it.isPresent }.mapValues { it.value.get() }.toMutableMap()
                    // for (k in availableResult.keys) ignite.put(k,availableResult[k].toString())
                    //ignite.mput(availableResult)
                    ignite.streamPut(availableResult)
                    layemetrics.MarkInRedis(availableResult.size.toLong(), metricType)

                   val notAvaialableResult = result.filterValues { !it.isPresent }.keys.toMutableList()
                    //for (k in notAvaialableResult)  ignite.addToNotInRedis(k)
                    //ignite.multiAddToNotInRedis(notAvaialableResult)
                    ignite.streamAddToNotInRedis(notAvaialableResult)
                    layemetrics.MarkNotInRedis(notAvaialableResult.size.toLong(), metricType)

                    keys.clear()
                    result.clear()
                    availableResult.clear()
                    notAvaialableResult.clear()

                    /*
                             if (result.result.isPresent) {
                                 layemetrics.MarkInRedis(1, metricType)
                                 val res = measureTimeMillis { ignite.put(key.result, result.result.get()) }
                                 logger.info { "ignite.put to main take ${res} millis." }
                             } else {
                                 layemetrics.MarkNotInRedis(1, metricType)
                                 val res = measureTimeMillis { ignite.addToNotInRedis(key.result) }
                                 logger.info { "ignite.put to NotInRadis take ${res} millis." }
                             }
         */
                } else {
                    Thread.sleep(1)
                }
            }
        }
    }

    fun add(key: String) {
        tmpQueue.offer(key)
    }
}
