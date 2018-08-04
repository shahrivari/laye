package ir.rkr.cacheservice


import com.typesafe.config.ConfigFactory
import ir.rkr.cacheservice.ignite.IgniteConnector
import ir.rkr.cacheservice.rest.JettyRestServer
import ir.rkr.cacheservice.util.LayeMetrics
import mu.KotlinLogging
import java.util.concurrent.TimeUnit
import com.codahale.metrics.ConsoleReporter





const val version = 0.2

/**
 * CacheService main entry point.
 */
fun main(args: Array<String>) {
    val logger = KotlinLogging.logger {}
    val config = ConfigFactory.defaultApplication()
    val layemetrics = LayeMetrics()
    val ignite = IgniteConnector(config,layemetrics)

    JettyRestServer(ignite, config, layemetrics)
    logger.info { "Laye V$version is ready :D" }



//    while (true){
//        println(layemetrics.CheckUrl.oneMinuteRate)
//
//        Thread.sleep(1000)
//    }
}
