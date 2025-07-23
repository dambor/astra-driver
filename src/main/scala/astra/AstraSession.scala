package astra

import java.nio.file.Paths
import com.datastax.oss.driver.api.core.CqlSession

object AstraSession extends SessionConfig {
  
  // Your existing methods (keep these unchanged)
  def createSession(keyspace: String): CqlSession = {
    CqlSession.builder()
      .withConfigLoader(loader)
      .withCloudSecureConnectBundle(Paths.get(Config.secureConnectionBundlePath))
      .withAuthCredentials("token", Config.getApplicationToken())
      .withKeyspace(keyspace)
      .build()
  }
  
  def createSession(): CqlSession = {
    CqlSession.builder()
      .withConfigLoader(loader)
      .withCloudSecureConnectBundle(Paths.get(Config.secureConnectionBundlePath))
      .withAuthCredentials("token", Config.getApplicationToken())
      .build()
  }
  
  // ADD these new methods to your existing object:
  def createSessionWithChaos(keyspace: String): (CqlSession, ChaosWrapper) = {
    val realSession = CqlSession.builder()
      .withConfigLoader(loader)
      .withCloudSecureConnectBundle(Paths.get(Config.secureConnectionBundlePath))
      .withAuthCredentials("token", Config.getApplicationToken())
      .withKeyspace(keyspace)
      .build()
    
    val chaosWrapper = new ChaosWrapper(realSession)
    (realSession, chaosWrapper)
  }
  
  def createSessionWithChaos(): (CqlSession, ChaosWrapper) = {
    val realSession = CqlSession.builder()
      .withConfigLoader(loader)
      .withCloudSecureConnectBundle(Paths.get(Config.secureConnectionBundlePath))
      .withAuthCredentials("token", Config.getApplicationToken())
      .build()
    
    val chaosWrapper = new ChaosWrapper(realSession)
    (realSession, chaosWrapper)
  }
}