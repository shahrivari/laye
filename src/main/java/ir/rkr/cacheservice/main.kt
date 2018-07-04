package ir.rkr.cacheservice


import com.typesafe.config.ConfigFactory
import ir.rkr.cacheservice.ignite.IgniteConnector
import ir.rkr.cacheservice.rest.JettyRestServer
import mu.KotlinLogging


const val version = 0.1

/**
 * CacheService main entry point.
 */
fun main(args: Array<String>) {
    val logger = KotlinLogging.logger {}
    val config = ConfigFactory.defaultApplication()
    val ignite = IgniteConnector(config)

    JettyRestServer(ignite, config)
    logger.info { "Laye V$version is ready :D" }

}
