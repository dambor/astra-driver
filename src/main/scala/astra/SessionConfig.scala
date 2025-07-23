package astra

import java.time.Duration

import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverConfigLoader}
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy

trait SessionConfig {

  val loader = DriverConfigLoader.programmaticBuilder()

    /**
     * set socket options
     */
    .withBoolean(DefaultDriverOption.SOCKET_KEEP_ALIVE, true)
    .withBoolean(DefaultDriverOption.SOCKET_TCP_NODELAY, true)

    /**
     * set request settings
     */
    .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM")
    .withBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE, true)
    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(Config.driverRequestTimeoutMs))

    /**
     * set load balancing option
     */
    .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, Config.localDC)
    .withClass(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, classOf[DefaultLoadBalancingPolicy])

    /**
     * set Pooling options
     */
    .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, Config.maxRequestPerConnection)
    .withInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_QUEUE_SIZE, Config.maxQueueSize)
    .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, Config.maxLocalConnectionPerhost)

    /**
     * Enabling Metrics for cassandra
     */
    .withString(DefaultDriverOption.METRICS_FACTORY_CLASS, "com.datastax.oss.driver.internal.core.metrics.DefaultMetricsFactory")
    .withStringList(DefaultDriverOption.METRICS_SESSION_ENABLED, java.util.Arrays.asList("cql-requests", "cql-client-timeouts"))
    .withStringList(DefaultDriverOption.METRICS_NODE_ENABLED, java.util.Arrays.asList(
      "pool.open-connections", "pool.in-flight",
      "errors.request.write-timeouts", "errors.request.read-timeouts",
      "retries.write-timeout", "retries.read-timeout"
    ))

    /**
     * enable warning logs
     */
    .withBoolean(DefaultDriverOption.REQUEST_LOG_WARNINGS, true)

    /**
     * Request tracking logging enabled
     */
    .withBoolean(DefaultDriverOption.REQUEST_LOGGER_SLOW_ENABLED, true)
    .withBoolean(DefaultDriverOption.REQUEST_LOGGER_ERROR_ENABLED, true)
    .withBoolean(DefaultDriverOption.REQUEST_LOGGER_SUCCESS_ENABLED, false)

    .endProfile()
    .build()
}
