package ir.rkr.cacheservice.ignite

import com.typesafe.config.Config
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCache
import org.apache.ignite.Ignition
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.cache.CacheWriteSynchronizationMode
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.configuration.DataStorageConfiguration
import org.apache.ignite.configuration.IgniteConfiguration
import java.util.*
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
//import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi


/**
 * [IgniteConnector] is service that starts up an ignite cache server as a member of ignite
 * cluster that specified in config file. Synchronization between nodes of cluster is being done
 * automatically in background. It can handle get and put for a key.
 */
class IgniteConnector(val config : Config){

    val ignite : Ignite
    val ignitecache : IgniteCache<String,String>

    /**
     * Setup a ignite cluster.
     */
    init {

        val spi = TcpDiscoverySpi()
        spi.setLocalAddress( config.getString("cacheservice.ignite.ip"))
        spi.setLocalPort(config.getInt("cacheservice.ignite.port"))

        val ipFinder = TcpDiscoveryVmIpFinder()
        ipFinder.setAddresses(config.getStringList("cacheservice.ignite.nodes"))
        spi.ipFinder = ipFinder

        val cache_name = config.getString("cacheservice.ignite.cachename")

        val cacheCfg = CacheConfiguration<String, String>(cache_name)
                .setCacheMode(CacheMode.REPLICATED)
                .setRebalanceBatchSize(10 * 1024 * 1024)
                .setRebalanceThrottle(0)
                .setRebalanceDelay(0)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC)

        val datacfg = DataStorageConfiguration()
                .setConcurrencyLevel(50)
                .setPageSize(8 * 1024)

        val cfg = IgniteConfiguration()
                .setServiceThreadPoolSize(10)
                .setDataStreamerThreadPoolSize(10)
                .setCacheConfiguration(cacheCfg)
                .setActiveOnStart(true)
                .setDataStorageConfiguration(datacfg)
                .setDiscoverySpi(spi)

        ignite = Ignition.start(cfg)
        ignitecache = ignite.getOrCreateCache<String, String>(cache_name)
    }

    /**
     * [put] is a function to put a key and value in cache.
     */
    fun put( key : String ,value : String){

        if (! ignitecache.isClosed)  ignitecache.put(key, value)
    }

    /**
     * [get] is a function to retrieve value of a key.
     */
    fun get (key: String ): Optional<String>  {

            val value = ignitecache.get(key)
            if (value == null) return Optional.empty<String>()
            return Optional.of(value)
    }
}