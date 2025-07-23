package astra

import com.datastax.oss.driver.api.core.CqlSession

/**
 * Base trait for cluster management
 */
trait BaseCluster {
  def getSession(): CqlSession
  def resetSession(): Unit
  def close(): Unit
}

/**
 * Cluster companion object for session management
 */
object Cluster {
  def getOrCreateLiveSession: BaseCluster = LiveSession
}

/**
 * Live session implementation
 */
object LiveSession extends BaseCluster {
  @volatile private var session: Option[CqlSession] = None
  private val lock = new Object()
  
  override def getSession(): CqlSession = {
    session.getOrElse {
      lock.synchronized {
        session.getOrElse {
          //val newSession = AstraSession.createSession()
          val newSession = AstraSession.createSession("tradingtech")  // With keyspace
          session = Some(newSession)
          newSession
        }
      }
    }
  }
  
  override def resetSession(): Unit = {
    lock.synchronized {
      session.foreach(_.close())
      session = None
    }
  }
  
  override def close(): Unit = {
    lock.synchronized {
      session.foreach(_.close())
      session = None
    }
  }
}