package ir.rkr.cacheservice.redis

import com.typesafe.config.Config
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool
import redis.clients.jedis.exceptions.JedisConnectionException
import java.security.SecureRandom
import java.util.*

val __secureRandom = SecureRandom()
inline fun <reified T> List<T>.randomItem() =
        if (isEmpty()) Optional.empty() else Optional.of(get(__secureRandom.nextInt(size)))

/**
 * [RedisConnector] is service that connects to List to redis servers and can only perform
 * get a key from redis servers.
 */
class RedisConnector(val config: Config) {

    private val jedisPoolList = mutableListOf<JedisPool>()

    init {
        val jediscfg = GenericObjectPoolConfig()

        jediscfg.maxIdle = 10
        jediscfg.maxTotal = 20
        jediscfg.minIdle = 5
        jediscfg.maxWaitMillis = 60000

        var pwd: String? = null
        if (config.hasPath("cacheservice.redis.password")) {

            pwd = config.getString("cacheservice.redis.password")
        }

        for (server in config.getStringList("cacheservice.redis.nodes")) {

            val pool = JedisPool(jediscfg, server, 6379,
                    1000, pwd, config.getInt("cacheservice.redis.db"))

            jedisPoolList.add(pool)
        }
    }

    /**
     * [get] is function for asking value of a key.
     */
    fun get(key: String): Optional<String> {

        val pool = jedisPoolList.randomItem().get()

        try {
            pool.resource.use { redis ->

                if (redis.isConnected) {

                    val value = redis.get(key)
                    if (value != null) return Optional.of(value)
                }
                return Optional.empty()
            }

        } catch (e: JedisConnectionException) {

            println("There is no resource in pool for redis.get.")
            return Optional.empty()
        }
    }
}