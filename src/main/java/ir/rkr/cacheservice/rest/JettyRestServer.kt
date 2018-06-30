package ir.rkr.cacheservice.rest

import com.google.common.util.concurrent.RateLimiter
import com.google.gson.GsonBuilder
import com.typesafe.config.Config
import ir.rkr.cacheservice.ignite.IgniteConnector
import ir.rkr.cacheservice.redis.RedisConnector
import ir.rkr.cacheservice.util.fromJson
import ir.rkr.cacheservice.version
import org.eclipse.jetty.http.HttpStatus
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import java.io.IOException
import javax.servlet.ServletException
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import kotlin.concurrent.thread

/**
 * [Results] is a data model for responses.
 */
data class Results(var results: HashMap<String, String> = HashMap<String, String>())

/**
 * [JettyRestServer] is a rest-based service to handle requests of redis cluster with an additional
 * in-memory cache layer based on ignite to increase performance and decrease number of requests of
 * redis cluster.
 */
class JettyRestServer(val ignite: IgniteConnector, val redis: RedisConnector, config: Config) : HttpServlet() {
    private val gson = GsonBuilder().disableHtmlEscaping().create()
    private val rateLimiter = RateLimiter.create(config.getDouble("rest.redisMaxRate"))
    /**
     * This function [checkDB] is used to ask value of a key from ignite server or redis server and update
     * ignite cluster.
     */
    private fun checkDB(key: String): String {
        var value = ignite.get(key)

        if (value.isPresent) {
            return value.get()
        } else {
            if (!rateLimiter.tryAcquire()) return ""
            thread {
                value = redis.get(key)
                if (value.isPresent) {
                    ignite.put(key, value.get())
                }
            }
            return ""
        }
    }

    /**
     * [doPost] can handle multi-get requests for keys in json format.
     */
    @Throws(ServletException::class, IOException::class)
    override fun doPost(req: HttpServletRequest, resp: HttpServletResponse) {
        val msg = Results()

        val parsedJson = gson.fromJson<Array<String>>(req.reader.readText())

        for (key in parsedJson) {
            msg.results[key] = checkDB(key)
        }

        resp.apply {
            status = HttpStatus.OK_200
            addHeader("Content-Type", "application/json; charset=utf-8")
            addHeader("Connection", "close")
            writer.write(gson.toJson(msg.results))
        }
    }

    /**
     * Start a jetty server.
     */
    init {
        val server = Server(config.getInt("rest.port"))
        val handler = ServletContextHandler(server, "/")
        handler.addServlet(ServletHolder(this), "/cache")
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