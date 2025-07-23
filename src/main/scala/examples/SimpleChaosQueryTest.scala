package examples

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import astra.{AstraSession, QueryExecutor, Cluster}
import com.datastax.oss.driver.api.core.cql.SimpleStatement

import scala.concurrent.ExecutionContext
import scala.util.{Success, Failure}
import scala.collection.mutable.Queue
import java.util.UUID

object SimpleChaosQueryTest extends App {
  implicit val system: ActorSystem = ActorSystem("SimpleChaosQueryTestSystem")
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val ec: ExecutionContext = system.dispatcher
  
  println("🐒🎯 Simple Chaos QueryExecutor Test - Finding Missing Records Bug...")
  
  try {
    // Create session with chaos wrapper
    val (realSession, chaosWrapper) = AstraSession.createSessionWithChaos("tradingtech")
    println("✅ Chaos session created!")
    
    // Create test table
    val createTable = SimpleStatement.newInstance(
      """
      CREATE TABLE IF NOT EXISTS simple_chaos_test (
        id UUID PRIMARY KEY,
        test_name TEXT,
        data TEXT,
        chaos_level TEXT,
        created_at TIMESTAMP
      )
      """
    )
    realSession.execute(createTable)
    println("✅ Table created!")
    
    // Prepare statement
    val insertStmt = realSession.prepare(
      "INSERT INTO simple_chaos_test (id, test_name, data, chaos_level, created_at) VALUES (?, ?, ?, ?, toTimestamp(now()))"
    )
    
    // Test 1: Normal operation using real session
    println("\n=== Test 1: Normal Operation (No Chaos) ===")
    chaosWrapper.disableChaos()
    testWithRealSession(realSession, insertStmt, "Normal", "0%", 5)
    
    Thread.sleep(2000)
    
    // Test 2: Test individual chaos wrapper calls
    println("\n=== Test 2: Individual Chaos Calls (30% Chaos) ===")
    chaosWrapper.enableChaos()
    chaosWrapper.setChaosLevel(0.3)
    testIndividualChaos(chaosWrapper, insertStmt, "IndividualChaos", "30%", 10)
    
    Thread.sleep(3000)
    
    // Test 3: Simulate what QueryExecutor does with chaos
    println("\n=== Test 3: Simulated QueryExecutor Batch (50% Chaos) ===")
    chaosWrapper.setChaosLevel(0.5)
    testSimulatedQueryExecutor(chaosWrapper, insertStmt, "BatchChaos", "50%", 15)
    
    Thread.sleep(5000)
    
    // Verification
    println("\n=== Verification: What Actually Got Inserted? ===")
    verifyActualInserts(realSession)
    
    // Show final stats
    println("\n=== Final Chaos Statistics ===")
    println(chaosWrapper.getStats)
    
    realSession.close()
    
  } catch {
    case ex: Exception =>
      log.error(ex, "Simple chaos test failed")
      println(s"❌ Test failed: ${ex.getMessage}")
      ex.printStackTrace()
  } finally {
    system.terminate()
  }
  
  def testWithRealSession(session: com.datastax.oss.driver.api.core.CqlSession,
                         insertStmt: com.datastax.oss.driver.api.core.cql.PreparedStatement,
                         testName: String,
                         chaosLevel: String,
                         recordCount: Int): Unit = {
    
    println(s"🧪 Testing with real session: $recordCount records...")
    
    (1 to recordCount).foreach { i =>
      val bound = insertStmt.bind(UUID.randomUUID(), testName, s"data_$i", chaosLevel)
      session.execute(bound)  // Sync call - no chaos
    }
    
    println(s"✅ Inserted $recordCount records using real session (no chaos)")
  }
  
  def testIndividualChaos(chaosWrapper: astra.ChaosWrapper,
                         insertStmt: com.datastax.oss.driver.api.core.cql.PreparedStatement,
                         testName: String,
                         chaosLevel: String,
                         recordCount: Int): Unit = {
    
    println(s"🧪 Testing individual chaos calls: $recordCount records...")
    
    var successCount = 0
    var failureCount = 0
    
    (1 to recordCount).foreach { i =>
      try {
        val bound = insertStmt.bind(UUID.randomUUID(), testName, s"data_$i", chaosLevel)
        
        // Use chaos wrapper's executeAsync
        val future = chaosWrapper.executeAsync(bound)
        val result = future.toCompletableFuture.get()  // Block to see immediate result
        
        successCount += 1
        println(s"   ✅ Record $i succeeded")
        
      } catch {
        case ex: Exception =>
          failureCount += 1
          println(s"   🐒 Record $i failed: ${ex.getMessage}")
      }
    }
    
    println(s"📊 Individual chaos results: $successCount successes, $failureCount failures")
  }
  
  def testSimulatedQueryExecutor(chaosWrapper: astra.ChaosWrapper,
                                insertStmt: com.datastax.oss.driver.api.core.cql.PreparedStatement,
                                testName: String,
                                chaosLevel: String,
                                recordCount: Int): Unit = {
    
    println(s"🧪 Simulating QueryExecutor batch processing: $recordCount records...")
    
    // Create async futures like QueryExecutor does
    val futures = (1 to recordCount).map { i =>
      val bound = insertStmt.bind(UUID.randomUUID(), testName, s"data_$i", chaosLevel)
      chaosWrapper.executeAsync(bound)
    }
    
    val startTime = System.currentTimeMillis()
    val statsBefore = chaosWrapper.getStats
    
    // Convert to Scala futures and use Future.sequence like QueryExecutor does
    import scala.concurrent.Future
    import scala.jdk.FutureConverters._
    
    val scalaFutures = futures.map(_.asScala)
    val combinedFuture = Future.sequence(scalaFutures)
    
    combinedFuture.onComplete {
      case Success(results) =>
        val duration = System.currentTimeMillis() - startTime
        val statsAfter = chaosWrapper.getStats
        val newChaos = statsAfter.chaosInjections - statsBefore.chaosInjections
        
        println(s"✅ Simulated QueryExecutor: SUCCESS for ${results.size} records in ${duration}ms")
        if (newChaos > 0) {
          println(s"🚨 CRITICAL BUG DETECTED: Future.sequence reported SUCCESS")
          println(s"🚨 BUT chaos injected $newChaos failures during batch!")
          println(s"🚨 This means some records may be missing from database!")
        } else {
          println(s"✅ No chaos injected - batch genuinely successful")
        }
        
      case Failure(ex) =>
        val duration = System.currentTimeMillis() - startTime
        val statsAfter = chaosWrapper.getStats
        val newChaos = statsAfter.chaosInjections - statsBefore.chaosInjections
        
        println(s"❌ Simulated QueryExecutor: FAILURE after ${duration}ms: ${ex.getMessage}")
        println(s"🐒 Chaos injected $newChaos failures - this caused the batch to fail")
        println(s"✅ This is correct behavior - batch properly failed due to chaos")
    }
  }
  
  def verifyActualInserts(session: com.datastax.oss.driver.api.core.CqlSession): Unit = {
    try {
      // Count actual records in database by chaos level
      val countQuery = SimpleStatement.newInstance("SELECT chaos_level, COUNT(*) as count FROM simple_chaos_test GROUP BY chaos_level")
      val results = session.execute(countQuery)
      
      println("📊 Actual records in database:")
      results.forEach { row =>
        val chaosLevel = row.getString("chaos_level")
        val count = row.getLong("count")
        println(s"   • $chaosLevel chaos: $count records")
      }
      
      // Total count
      val totalQuery = SimpleStatement.newInstance("SELECT COUNT(*) as total FROM simple_chaos_test")
      val totalResult = session.execute(totalQuery).one()
      val totalCount = totalResult.getLong("total")
      println(s"   📊 TOTAL RECORDS: $totalCount")
      
    } catch {
      case ex: Exception =>
        println(s"❌ Verification failed: ${ex.getMessage}")
    }
  }
}