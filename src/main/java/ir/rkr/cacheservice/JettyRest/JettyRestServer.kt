package ir.rkr.cacheservice.JettyRest

import com.google.gson.GsonBuilder
import com.typesafe.config.Config
import ir.rkr.cacheservice.ignite.IgniteConnector
import ir.rkr.cacheservice.redis.RedisConnector
import org.eclipse.jetty.http.HttpStatus
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import java.io.IOException
import javax.servlet.ServletException
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

/**
 * [Keys] is a data model for requests.
 */
data class Keys(var keys: Array<String>)

/**
 * [Results] is a data model for responses.
 */
data class Results(var results: HashMap<String, String> = HashMap<String, String>())

/**
 * [JettyRestServer] is a rest-based service to handle requests of redis cluster with an additional
 * in-memory cache layer based on ignite to increase performance and decrease number of requests of
 * redis cluster.
 */
class JettyRestServer(val ignite: IgniteConnector, val redis: RedisConnector, val config: Config) : HttpServlet() {

    val gson = GsonBuilder().create()

    /**
     * This function [checkDB] is used to ask value of a key from ignite server or redis server and update
     * ignite cluster.
     */
    private fun checkDB(key: String): String {

        var value = ignite.get(key)

        if (value.isPresent) {
            return value.get()
        } else {
            value = redis.get(key)
            if (value.isPresent) {
                ignite.put(key, value.get())
                return value.get()
            } else
                return ""
        }
    }

    /**
     * [doGet] can handle multi-get requests for keys in json format.
     */
    @Throws(ServletException::class, IOException::class)
    override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {

        var msg = Results()
        val parsedjson = gson.fromJson(req.reader.readLine(), Keys::class.java)

        for (key in parsedjson.keys.asIterable()) {
            msg.results.set(key, checkDB(key))
        }

        resp.status = HttpStatus.OK_200
        resp.writer.println(gson.toJson(msg.results))
    }


    /**
     * Start a jetty server.
     */
    init {
        val server = Server(config.getInt("cacheservice.rest.port"))
        val handler = ServletContextHandler(server, "/")
        handler.addServlet(ServletHolder(this), "/cache")
        server.start()
    }

}