package ir.rkr.cacheservice.ignite

import com.google.common.collect.EvictingQueue
import ir.rkr.cacheservice.redis.RedisConnector
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import kotlin.concurrent.thread


internal class IgniteFeeder(ignite: IgniteConnector, redis: RedisConnector, queueSize: Int = 100_000) {


    var tmpQueue: BlockingQueue<String> = ArrayBlockingQueue(queueSize)

    init {
        thread {
            while (true) {
                val key = tmpQueue.poll()
                if (!key.isNullOrBlank()) {
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
        tmpQueue.offer(key)
    }
}
