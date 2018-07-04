package ir.rkr.cacheservice.ignite

import com.google.common.collect.EvictingQueue
import ir.rkr.cacheservice.redis.RedisConnector
import kotlin.concurrent.thread


internal class IgniteFeeder(ignite: IgniteConnector, redis: RedisConnector, queueSize: Int = 100_000) {

    val tmpQueue = EvictingQueue.create<String>(queueSize)

    init {
        thread {
            while (true) {
                val key = tmpQueue.poll()
                if (!key.isNullOrEmpty()) {
                    val value = redis.get(key)
                    if (value.isPresent) {
                        ignite.put(key, value.get())
                    }
                    else ignite.addToNotInRedis(key)
                }
                Thread.sleep(1)
            }
        }
    }

    fun add(key: String) {
        tmpQueue.add(key)
    }
}
