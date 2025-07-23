package examples

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import astra.AstraSession
import com.datastax.oss.driver.api.core.cql.SimpleStatement

import scala.concurrent.{ExecutionContext}
import scala.util.{Success, Failure, Try}
import scala.jdk.CollectionConverters._

object SimpleDemoApp extends App {
  
  implicit val system: ActorSystem = ActorSystem("SimpleDemoSystem")
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val ec: ExecutionContext = system.dispatcher
  
  println("🚀 Simple Astra Demo Starting...")
  
  try {
    // Connect without specifying keyspace to see what's available
    println("🔗 Connecting to Astra...")
    val session = AstraSession.createSession()
    
    println("✅ Successfully connected to Astra DB!")
    
    // List available keyspaces
    println("📋 Available keyspaces:")
    val keyspaceQuery = SimpleStatement.newInstance(
      "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name NOT IN ('system', 'system_auth', 'system_distributed', 'system_schema', 'system_traces', 'system_views', 'data_endpoint_auth')"
    )
    
    val keyspaces = session.execute(keyspaceQuery)
    val keyspaceNames = keyspaces.asScala.map(_.getString("keyspace_name")).toList
    
    if (keyspaceNames.nonEmpty) {
      keyspaceNames.foreach(ks => println(s"   • $ks"))
      
      // Use the first available keyspace
      val targetKeyspace = keyspaceNames.head
      println(s"🎯 Using keyspace: $targetKeyspace")
      
      // Reconnect with the keyspace
      session.close()
      val keyspaceSession = AstraSession.createSession(targetKeyspace)
      
      // Create a simple table
      println("📋 Creating demo table...")
      val createTable = SimpleStatement.newInstance(
        """
        CREATE TABLE IF NOT EXISTS demo_users (
          id UUID PRIMARY KEY,
          name TEXT,
          email TEXT,
          created_at TIMESTAMP
        )
        """
      )
      keyspaceSession.execute(createTable)
      println("✅ Table created successfully!")
      
      // Insert a test record
      println("📝 Inserting test data...")
      val insertData = SimpleStatement.newInstance(
        "INSERT INTO demo_users (id, name, email, created_at) VALUES (uuid(), ?, ?, toTimestamp(now()))",
        "John Doe", "john@example.com"
      )
      keyspaceSession.execute(insertData)
      println("✅ Data inserted successfully!")
      
      // Query the data
      println("🔍 Querying data...")
      val selectData = SimpleStatement.newInstance("SELECT * FROM demo_users LIMIT 5")
      val results = keyspaceSession.execute(selectData)
      
      println("📊 Results:")
      results.asScala.foreach { row =>
        println(s"   • ${row.getString("name")} - ${row.getString("email")}")
      }
      
      keyspaceSession.close()
      println("🎉 Demo completed successfully!")
      
    } else {
      println("❌ No user keyspaces found. You may need to:")
      println("1. Create a keyspace in the Astra Console")
      println("2. Or check your token permissions")
    }
    
    session.close()
    
  } catch {
    case ex: Exception =>
      log.error(ex, "Demo failed")
      println(s"❌ Demo failed: ${ex.getMessage}")
      println("\n🔧 Troubleshooting:")
      println("1. Make sure ASTRA_DB_APPLICATION_TOKEN is set")
      println("2. Check that your token has read/write permissions")
      println("3. Verify your secure bundle path is correct")
  } finally {
    system.terminate()
  }
}