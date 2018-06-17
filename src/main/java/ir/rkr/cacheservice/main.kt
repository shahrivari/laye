package ir.rkr.cacheservice

import com.typesafe.config.ConfigFactory
import ir.rkr.cacheservice.JettyRest.JettyRestServer
import ir.rkr.cacheservice.ignite.IgniteConnector
import ir.rkr.cacheservice.redis.RedisConnector
import java.io.File


/**
 * CacheService main entry point.
 */
fun main(args: Array<String>) {

    val configFile = File(System.getProperty("configFile"))
    val parsedConfig = ConfigFactory.parseFile(configFile)
    val config = ConfigFactory.load(parsedConfig)
    val redis = RedisConnector(config)
    val ignite = IgniteConnector(config)

    JettyRestServer(ignite, redis, config)

}
