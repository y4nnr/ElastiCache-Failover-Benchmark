package elasticache.test

import org.redisson.Redisson
import org.redisson.api.RedissonClient
import org.redisson.config.BaseConfig
import org.redisson.config.BaseMasterSlaveServersConfig
import org.redisson.config.Config
import org.redisson.config.ReadMode

object RedissonCommandExecutorFactory {

    private fun fillBaseConfig(
            config: BaseConfig<*>,
            password: String? = null,
            socketTimeoutMs: Int? = null,
            connectionTimeoutMs: Int? = null
    ) {
        config.setPassword(password)
        config.setTimeout(socketTimeoutMs ?: SO_TIMEOUT_MS)
        config.setConnectTimeout(connectionTimeoutMs ?: CONNECTION_TIMEOUT_MS)
        config.setTcpNoDelay(true)
        config.setKeepAlive(true)
    }

    fun createCluster(
            nodeUris: List<String>,
            password: String? = null
    ): RedissonClient {
        val config = Config().apply {
            nettyThreads = NETTY_THREADS
        }
        config.useClusterServers().apply {
            addNodeAddress(*nodeUris.toTypedArray())
            isKeepAlive = true
            readMode = ReadMode.MASTER_SLAVE
            fillBaseConfig(this, password)
            dnsMonitoringInterval = DNS_MONITORING_INTERVAL_MS
            scanInterval = CLUSTER_SCAN_INTERVAL_MS
            setConnectionPoolSize(this, 10)
            setSlaveCheckParams(this)
        }

        return Redisson.create(config)
    }

    private fun setConnectionPoolSize(
            config: BaseMasterSlaveServersConfig<*>,
            minConnection: Int
    ) {
        config.setMasterConnectionMinimumIdleSize(1)
        config.setMasterConnectionPoolSize(minConnection)
        config.setSlaveConnectionMinimumIdleSize(1)
        config.setSlaveConnectionPoolSize(minConnection)
    }

    private fun <T : BaseMasterSlaveServersConfig<T>> setSlaveCheckParams(
            config: BaseMasterSlaveServersConfig<T>
    ) {
        config.failedSlaveCheckInterval = FAILED_SLAVE_CHECK_INTERVAL_MS
        config.failedSlaveReconnectionInterval = FAILED_SLAVE_RECONNECTION_INTERVAL_MS
    }

    private const val DNS_MONITORING_INTERVAL_MS = 3000L
    private const val FAILED_SLAVE_RECONNECTION_INTERVAL_MS = 1000
    private const val FAILED_SLAVE_CHECK_INTERVAL_MS = 3000
    private const val SO_TIMEOUT_MS = 5000
    private const val CONNECTION_TIMEOUT_MS = 5000

    /**
     * how often redisson check cluster topology
     */
    private const val CLUSTER_SCAN_INTERVAL_MS = 1000

    private const val NETTY_THREADS = 3
}