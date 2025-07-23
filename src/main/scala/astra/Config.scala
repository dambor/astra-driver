package astra

import com.typesafe.config.ConfigFactory

object Config {
  private val config = ConfigFactory.load()
  
  // Driver configuration
  val driverRequestTimeoutMs: Long = config.getLong("astra.driver.request-timeout-ms")
  val localDC: String = config.getString("astra.driver.local-datacenter")
  val maxRequestPerConnection: Int = config.getInt("astra.driver.max-requests-per-connection")
  val maxQueueSize: Int = config.getInt("astra.driver.max-queue-size")
  val maxLocalConnectionPerhost: Int = config.getInt("astra.driver.max-local-connections-per-host")
  
  // Astra connection configuration
  val secureConnectionBundlePath: String = config.getString("astra.connection.secure-bundle-path")
  
  // Modern authentication using Application Token
  def getApplicationToken(): String = sys.env.getOrElse("ASTRA_DB_APPLICATION_TOKEN", 
    config.getString("astra.auth.application-token"))
}