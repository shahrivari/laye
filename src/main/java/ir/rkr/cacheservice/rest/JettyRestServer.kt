package ir.rkr.cacheservice.rest

import com.google.common.util.concurrent.RateLimiter
import com.google.gson.GsonBuilder
import com.typesafe.config.Config
import ir.rkr.cacheservice.ignite.IgniteConnector
import ir.rkr.cacheservice.ignite.IgniteFeeder
import ir.rkr.cacheservice.redis.RedisConnector
import ir.rkr.cacheservice.util.LayeMetrics
import ir.rkr.cacheservice.util.fromJson
import ir.rkr.cacheservice.version
import mu.KotlinLogging
import org.eclipse.jetty.http.HttpStatus
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.util.thread.QueuedThreadPool
import java.util.concurrent.atomic.AtomicLong
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse


/**
 * [Results] is a data model for responses.
 */
data class Results(var results: HashMap<String, String> = HashMap<String, String>())

/**
 * [JettyRestServer] is a rest-based service to handle requests of redis cluster with an additional
 * in-memory cache layer based on ignite to increase performance and decrease number of requests of
 * redis cluster.
 */
class JettyRestServer(val ignite: IgniteConnector, val config: Config, val layemetrics: LayeMetrics) : HttpServlet() {

    private val gson = GsonBuilder().disableHtmlEscaping().create()
    private val urlRateLimiter = RateLimiter.create(config.getDouble("redis.url.rateLimit"))
    private val tagRateLimiter = RateLimiter.create(config.getDouble("redis.tag.rateLimit"))
    private val redisUrl = RedisConnector(config.getConfig("redis.url"))
    private val redisTag = RedisConnector(config.getConfig("redis.tag"))
    private val urlQueue = IgniteFeeder(ignite, redisUrl, 100_000, layemetrics, "URL")
    private val tagQueue = IgniteFeeder(ignite, redisTag, 100_000, layemetrics, "TAG")
    private val logger = KotlinLogging.logger {}
    /**
     * This function [checkUrl] is used to ask value of a key from ignite server or redis server and update
     * ignite cluster.
     */
    private val churlCount = AtomicLong()

    private fun checkUrl(key: String): String {

        layemetrics.MarkCheckUrl(1)

        if (ignite.isNotInRedis(key)) {
            layemetrics.MarkUrlIn5min(1)
            return ""
        }
        var value = ignite.get(key)

       /* if (churlCount.incrementAndGet() % 10000L == 0L)
            logger.info { "calls to ignite ${churlCount.get()}" }*/
        if (value.isPresent) {
            layemetrics.MarkUrlInIgnite(1)
            if (Math.random() < config.getDouble("redis.url.sslRatio"))
                return value.get()
                    .replace("http://","https://")
            return value.get()
        } else {
            layemetrics.MarkUrlNotInIgnite(1)
            if (!urlRateLimiter.tryAcquire()) return ""
            urlQueue.add(key)
            return ""
        }
    }

    /**
     * This function [checkTag] is used to ask value of a key from ignite server or redis server and update
     * ignite cluster.
     */
    private fun checkTag(key: String): String {

        layemetrics.MarkCheckTag(1)

        if (ignite.isNotInRedis(key)) {
            layemetrics.MarkTagIn5min()
            return ""
        }
        var value = ignite.get(key)

        if (value.isPresent) {
            layemetrics.MarkTagInIgnite(1)
            return value.get()
        } else {
            layemetrics.MarkTagNotInIgnite(1)
            if (!tagRateLimiter.tryAcquire()) return ""
            tagQueue.add(key)
            return ""
        }
    }

    /**
     * Start a jetty server.
     */
    init {
        val threadPool = QueuedThreadPool(400, 20)
        val server = Server(threadPool)

        // config.getInt("rest.port")

        val http = ServerConnector(server).apply { port = config.getInt("rest.port") }

        server.addConnector(http)


        val handler = ServletContextHandler(server, "/")

        /**
         * It can handle multi-get requests for Urls in json format.
         */
        handler.addServlet(ServletHolder(object : HttpServlet() {
            override fun doPost(req: HttpServletRequest, resp: HttpServletResponse) {
                val msg = Results()

                val parsedJson = gson.fromJson<Array<String>>(req.reader.readText())

                layemetrics.MarkUrlBatches(1)
                for (key in parsedJson) {

                    if (key != null) msg.results[key] = checkUrl(key)
                }

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/json; charset=utf-8")
                    //addHeader("Connection", "close")
                    writer.write(gson.toJson(msg.results))
                }
            }
        }), "/cache/url")

        /**
         * It can handle multi-get requests for Tags in json format.
         */
        handler.addServlet(ServletHolder(object : HttpServlet() {
            override fun doPost(req: HttpServletRequest, resp: HttpServletResponse) {
                val msg = Results()

                val parsedJson = gson.fromJson<Array<String>>(req.reader.readText())
                layemetrics.MarkTagBatches(1)
                for (key in parsedJson) {
                    if (key != null) msg.results[key] = checkTag(key)
                }

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/json; charset=utf-8")
                    //addHeader("Connection", "close")
                    writer.write(gson.toJson(msg.results))
                }
            }
        }), "/cache/tag")


        handler.addServlet(ServletHolder(object : HttpServlet() {
            override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/json; charset=utf-8")
                    //addHeader("Connection", "close")
                    writer.write(gson.toJson(layemetrics.getInfo()))
                }
            }
        }), "/metrics")

        handler.addServlet(ServletHolder(object : HttpServlet() {
            override fun doPost(req: HttpServletRequest, resp: HttpServletResponse) {

                val parsedJson = gson.fromJson<Array<String>>(req.reader.readText())

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/json; charset=utf-8")
                    //addHeader("Connection", "close")
                    writer.write(gson.toJson(ignite.query(parsedJson[0])))
                }
            }
        }), "/query")

        handler.addServlet(ServletHolder(object : HttpServlet() {
            override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {
                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "text/plain; charset=utf-8")
                    addHeader("Connection", "close")
                    writer.write("Laye V$version is running :D")
                }
            }
        }), "/health")

        server.start()

    }
}