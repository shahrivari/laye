package ir.rkr.cacheservice.ignite

import ir.rkr.cacheservice.redis.RedisConnector
import ir.rkr.cacheservice.util.LayeMetrics
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import kotlin.concurrent.thread


internal class IgniteFeeder(ignite: IgniteConnector, redis: RedisConnector, queueSize: Int = 100_000,
                            val layemetrics: LayeMetrics, metricType: String) {


    var tmpQueue: BlockingQueue<String> = ArrayBlockingQueue(queueSize)

    init {
        thread {
            while (true) {
                val key = tmpQueue.poll()
                if (!key.isNullOrBlank()) {
                    val value = redis.get(key)
                    if (value.isPresent) {
                        layemetrics.MarkInRedis(1, metricType)
                        ignite.put(key, value.get())
                    } else {
                        layemetrics.MarkNotInRedis(1, metricType)
                        ignite.addToNotInRedis(key)
                    }
                }
                Thread.sleep(1)
            }
        }
    }

    fun add(key: String) {
        tmpQueue.offer(key)
    }
}