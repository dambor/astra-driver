package astra

import java.nio.file.Paths

import com.datastax.oss.driver.api.core.CqlSession

object AstraSession extends SessionConfig {
  
  /**
   * Creates an Astra DB session with token authentication (modern method)
   * @param keyspace The keyspace to connect to
   * @return CqlSession instance
   */
  def createSession(keyspace: String): CqlSession = {
    CqlSession.builder()
      .withConfigLoader(loader)
      .withCloudSecureConnectBundle(Paths.get(Config.secureConnectionBundlePath))
      .withAuthCredentials("token", Config.getApplicationToken())
      .withKeyspace(keyspace)
      .build()
  }
  
  /**
   * Creates an Astra DB session without specifying a keyspace
   * @return CqlSession instance
   */
  def createSession(): CqlSession = {
    CqlSession.builder()
      .withConfigLoader(loader)
      .withCloudSecureConnectBundle(Paths.get(Config.secureConnectionBundlePath))
      .withAuthCredentials("token", Config.getApplicationToken())
      .build()
  }
}