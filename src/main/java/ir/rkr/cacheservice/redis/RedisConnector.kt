package ir.rkr.cacheservice.redis

import com.typesafe.config.Config
import ir.rkr.cacheservice.util.randomItem
import mu.KotlinLogging
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool
import java.util.*
import kotlin.system.exitProcess

/**
 * [RedisConnector] is service that connects to List to redis servers and can only perform
 * get a key from redis servers.
 */
class RedisConnector(config: Config) {
    private val logger = KotlinLogging.logger {}
    private val jedisPoolList = mutableListOf<JedisPool>()

    init {
        val jedisCfg = GenericObjectPoolConfig().apply {
            maxIdle = 200
            maxTotal = 400
            minIdle = 50
            maxWaitMillis = 800
        }

        var pwd: String? = null
        if (config.hasPath("password"))  pwd = config.getString("password")


        var numActiveRedis = 0
        val nodeList = config.getStringList("nodes")

        require(nodeList.isNotEmpty()) { "No redis server specified in config file." }

        for (server in nodeList) {
            val pool = JedisPool(jedisCfg, server, config.getInt("port"),
                    800, pwd, config.getInt("db"))

            try {
                pool.resource.use {}
                numActiveRedis += 1

            } catch (e: Exception) {
                logger.error(e) { "Redis is not available on ${server}." }
            }

            jedisPoolList.add(pool)
        }

        require(numActiveRedis > 0) { "There is no Active Redis server ?!?! Bye Bye" }

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

        } catch (e: Exception) {
            logger.trace(e) { "There is no resource in pool for redis.get." }
            return Optional.empty()
        }
    }

    /**
     * [get] is function for asking value of a key.
     */
    fun mget(keys: List<String>): MutableMap<String, Optional<String>> {
        val pool = jedisPoolList.randomItem().get()

        var result = keys.map { it to Optional.empty<String>() }.toMap().toMutableMap()
        try {
            pool.resource.use { redis ->
                if (redis.isConnected) {
                    val values = redis.mget(*keys.toTypedArray())
//                    if (value != null) return Optional.of(value)
                    result = keys.zip(values).toMap().mapValues {
                        if (it.value == null)
                            Optional.empty<String>()
                        else
                            Optional.of(it.value)
                    }.toMutableMap()
                }
                return result
            }

        } catch (e: Exception) {
            logger.trace(e) { "There is no resource in pool for redis.mget." }
            return result
        }
    }
}