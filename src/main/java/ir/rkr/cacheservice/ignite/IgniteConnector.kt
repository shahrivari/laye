package ir.rkr.cacheservice.ignite

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import com.typesafe.config.Config
import ir.rkr.cacheservice.util.LayeMetrics
import mu.KotlinLogging
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCache
import org.apache.ignite.Ignition
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.cache.CacheWriteSynchronizationMode
import org.apache.ignite.configuration.*
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.function.Supplier


/**
 * [IgniteConnector] is service that starts up an ignite cache server as a member of ignite
 * cluster that specified in config file. Synchronization between nodes of cluster is being done
 * automatically in background. It can handle get and put for a key.
 */
class IgniteConnector(config: Config,layemetrics :LayeMetrics) {
    private val logger = KotlinLogging.logger {}
    val ignite: Ignite
    val igniteCache: IgniteCache<String, String>
    val notInRedis: Cache<String, Int>
    val cacheName: String

    /**
     * Setup a ignite cluster.
     */
    init {

        val spi = TcpDiscoverySpi()
        spi.localAddress = config.getString("ignite.ip")
        spi.localPort = config.getInt("ignite.port")

        val ipFinder = TcpDiscoveryVmIpFinder()
        ipFinder.setAddresses(config.getStringList("ignite.nodes"))
        spi.ipFinder = ipFinder

        cacheName = config.getString("ignite.cacheName")


        val cacheCfg = CacheConfiguration<String, String>(cacheName)
                .setCacheMode(CacheMode.REPLICATED)
                .setRebalanceBatchSize(10 * 1024 * 1024)
                .setRebalanceThrottle(0)
                .setRebalanceDelay(0)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
//                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(
//                        Duration(TimeUnit.MINUTES,config.getLong("ignite.ttl"))))


        val storageCfg = DataStorageConfiguration()
                .setConcurrencyLevel(100)
                .setPageSize(16 * 1024)

        val regionCfg = DataRegionConfiguration()

        regionCfg.name = "90GB_Region"
        regionCfg.initialSize = 5000L * 1024 * 1024
        regionCfg.maxSize = 90L * 1024 * 1024 * 1024
        regionCfg.pageEvictionMode = DataPageEvictionMode.RANDOM_LRU

        storageCfg.setDataRegionConfigurations(regionCfg)

        val cfg = IgniteConfiguration()
                .setServiceThreadPoolSize(300)
                .setDataStreamerThreadPoolSize(300)
                .setCacheConfiguration(cacheCfg)
                .setActiveOnStart(true)
                .setDataStorageConfiguration(storageCfg)
                .setDiscoverySpi(spi)

        ignite = Ignition.start(cfg)

        //require(cacheName != "NotInRedis")
        igniteCache = ignite.getOrCreateCache<String, String>(cacheName)
        notInRedis = CacheBuilder.newBuilder()
              //  .expireAfterWrite(config.getLong("ignite.ttlForNotInRedis"),TimeUnit.MINUTES)
                .maximumSize(config.getLong("ignite.guavaNotInRedis"))
                .build<String, Int>()
        layemetrics.addGauge("GuavaSize", Supplier { notInRedis.size() })

    }


    fun streamPut(input: Map<String, String>) {

        try {
            ignite.dataStreamer<String, String>(cacheName).use {
                for ((key, value) in input) {
                    it.addData(key, value)
                }
            }
        } catch (e: Exception) {
            logger.error(e) { "Error in streamPut" }
        }

    }

    fun streamAddToNotInRedis(input: List<String>) {
        try {
            for (key in input) {
                notInRedis.put(key, 0)
            }
        } catch (e: Exception) {
            logger.error(e) { "Error in streamPut" }
        }

    }

    /**
     * [put] is a function to put a key and value in cache.
     */
    fun put(key: String, value: String) {
        try {
            if (!igniteCache.isClosed) igniteCache.putAsync(key, value)
        } catch (e: Exception) {
            logger.error(e) { "Error in putting value:$value for key:$key into Ignite." }
        }
    }

    /**
     * [mput] is a function to put a key and value in cache.
     */
    fun mput(input: Map<String, String>) {
        try {
            if (!igniteCache.isClosed) igniteCache.putAll(input)
        } catch (e: Exception) {
            logger.error(e) { "Error in Mputting to ignite." }
        }
    }

    /**
     * [get] is a function to retrieve value of a key.
     */
    fun get(key: String): Optional<String> {
        try {
            val value = igniteCache.get(key)
            if (value == null) return Optional.empty<String>()
            return Optional.of(value)
        } catch (e: Exception) {
            logger.error(e) { "Error in reading value for key:$key from Ignite." }
            return Optional.empty()
        }
    }


    fun isNotInRedis(key: String): Boolean {

        return notInRedis.getIfPresent(key) != null

    }

}