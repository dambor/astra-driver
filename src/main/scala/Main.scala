import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import astra.{AstraSession, AstraUploader, Cluster}
import com.datastax.oss.driver.api.core.cql.SimpleStatement

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success, Try}

object Main extends App {
  
  implicit val system: ActorSystem = ActorSystem("AstraDriverSystem")
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val ec: ExecutionContext = system.dispatcher
  
  println("Astra Driver Application Starting...")
  
  try {
    // Example usage of the Astra driver
    val session = AstraSession.createSession("your_keyspace")
    
    // Test connection
    val testQuery = SimpleStatement.newInstance("SELECT release_version FROM system.local")
    val resultSet = session.execute(testQuery)
    val version = resultSet.one().getString("release_version")
    
    log.info(s"Connected to Cassandra version: $version")
    println(s"Successfully connected to Astra DB! Cassandra version: $version")
    
    // Example of using the uploader
    val uploader = new AstraUploader()
    
    // Clean shutdown
    session.close()
    
  } catch {
    case e: Exception =>
      log.error(e, "Failed to connect to Astra DB")
      println(s"Failed to connect: ${e.getMessage}")
      println("Make sure to:")
      println("1. Set ASTRA_CLIENT_ID environment variable")
      println("2. Set ASTRA_CLIENT_SECRET environment variable") 
      println("3. Update the secure-bundle-path in application.conf")
  } finally {
    Try {
      Cluster.getOrCreateLiveSession.close()
      Await.ready(system.terminate(), 10.seconds)
    }
  }
}
