package examples

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import astra.{AstraSession, AstraUploader, Cluster}
import com.datastax.oss.driver.api.core.cql.{SimpleStatement, BoundStatement}

import scala.concurrent.ExecutionContext
import scala.collection.mutable.Queue
import java.util.UUID

object SimpleVerificationTest extends App {
  implicit val system: ActorSystem = ActorSystem("SimpleVerificationTestSystem")
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val ec: ExecutionContext = system.dispatcher
  
  println("🐒🧹 Simple Verification Test - Final Bug Check...")
  
  try {
    // Create session with chaos wrapper  
    val (realSession, chaosWrapper) = AstraSession.createSessionWithChaos("tradingtech")
    println("✅ Chaos session created!")
    
    // 🧹 CLEAR the table first
    println("🧹 Clearing test table for fresh start...")
    val dropTable = SimpleStatement.newInstance("DROP TABLE IF EXISTS simple_verification_test")
    realSession.execute(dropTable)
    println("✅ Old table dropped!")
    
    // Create simple test table (no filtering needed)
    val createTable = SimpleStatement.newInstance(
      """
      CREATE TABLE IF NOT EXISTS simple_verification_test (
        id UUID PRIMARY KEY,
        message_data TEXT,
        topic TEXT,
        created_at TIMESTAMP
      )
      """
    )
    realSession.execute(createTable)
    println("✅ Fresh table created!")
    
    // Use cluster session for consistency
    val clusterSession = Cluster.getOrCreateLiveSession.getSession()
    
    // Prepare statement on the same session that will execute it
    val insertStmt = clusterSession.prepare(
      """
      INSERT INTO simple_verification_test (id, message_data, topic, created_at) 
      VALUES (?, ?, ?, toTimestamp(now()))
      """
    )
    println("✅ Statement prepared on cluster session!")
    
    // Create your actual AstraUploader
    val uploader = new AstraUploader()
    
    // Count records before test
    val countBefore = countRecords(clusterSession)
    println(s"📊 Records before test: $countBefore")
    
    // Test 1: Normal operation (no chaos)
    println(s"\n=== Test 1: Normal Operation ===")
    chaosWrapper.disableChaos()
    testAstraUploaderBatch(uploader, clusterSession, chaosWrapper, insertStmt, "Normal", "normal_topic", 5)
    
    Thread.sleep(5000)  // Wait for async completion
    
    // Check count after test 1
    val countAfterTest1 = countRecords(clusterSession)
    println(s"📊 Records after test 1: $countAfterTest1 (should be ${countBefore + 5})")
    
    // Test 2: Low chaos 
    println(s"\n=== Test 2: 20% Chaos ===")
    chaosWrapper.enableChaos()
    chaosWrapper.setChaosLevel(0.2)
    testAstraUploaderBatch(uploader, clusterSession, chaosWrapper, insertStmt, "LowChaos", "low_chaos_topic", 10)
    
    Thread.sleep(10000)  // Wait longer for retries
    
    // Check count after test 2
    val countAfterTest2 = countRecords(clusterSession)
    println(s"📊 Records after test 2: $countAfterTest2 (should be ${countAfterTest1 + 10})")
    
    // Test 3: Medium chaos 
    println(s"\n=== Test 3: 40% Chaos ===")
    chaosWrapper.setChaosLevel(0.4)
    testAstraUploaderBatch(uploader, clusterSession, chaosWrapper, insertStmt, "MediumChaos", "medium_chaos_topic", 15)
    
    Thread.sleep(15000)  // Wait even longer for retries
    
    // Final verification
    val finalCount = countRecords(clusterSession)
    println(s"\n=== Final Verification ===")
    println(s"📊 Final record count: $finalCount")
    
    val expectedTotal = countBefore + 5 + 10 + 15
    
    if (finalCount == expectedTotal) {
      println(s"🎉 SUCCESS: Found expected $expectedTotal records!")
      println(s"✅ Your missing records bug is COMPLETELY FIXED!")
      println(s"🚀 Session consistency fix works perfectly!")
    } else if (finalCount < expectedTotal) {
      println(s"🚨 MISSING RECORDS: Expected $expectedTotal, found $finalCount")
      println(s"🚨 Missing ${expectedTotal - finalCount} records - there may be other issues")
    } else {
      println(s"🔄 EXTRA RECORDS: Expected $expectedTotal, found $finalCount") 
      println(s"🔄 Extra ${finalCount - expectedTotal} records - potential retry logic issue")
    }
    
    // Show some sample data
    showSampleRecords(clusterSession)
    
    // Show final chaos statistics
    println("\n=== Final Chaos Statistics ===")
    println(chaosWrapper.getStats)
    
    clusterSession.close()
    
  } catch {
    case ex: Exception =>
      log.error(ex, "Simple verification test failed")
      println(s"❌ Test failed: ${ex.getMessage}")
      ex.printStackTrace()
  } finally {
    println("🔚 Terminating actor system...")
    system.terminate()
  }
  
  def testAstraUploaderBatch(uploader: AstraUploader,
                            clusterSession: com.datastax.oss.driver.api.core.CqlSession,
                            chaosWrapper: astra.ChaosWrapper,
                            insertStmt: com.datastax.oss.driver.api.core.cql.PreparedStatement,
                            testName: String,
                            topic: String,
                            recordCount: Int): Unit = {
    
    println(s"🧪 Testing AstraUploader '$testName' with $recordCount records on topic '$topic'...")
    
    val batchId = UUID.randomUUID().toString
    
    // Create elements in the format AstraUploader expects: Seq[(Any, BoundStatement)]
    val elements = (1 to recordCount).map { i =>
      val messageData = s"${testName}_message_$i"
      val boundStmt = insertStmt.bind(
        UUID.randomUUID(),
        messageData,
        topic
      )
      (messageData, boundStmt)  // (Any, BoundStatement)
    }
    
    // Create successfulBatch in the format AstraUploader expects: Queue[BoundStatement]
    val successfulBatch = Queue(elements.map(_._2): _*)
    
    val statsBefore = chaosWrapper.getStats
    
    println(s"📤 Calling AstraUploader.processBatch...")
    println(s"   • Records: $recordCount")
    println(s"   • Topic: $topic")
    println(s"   • Chaos Level: ${(chaosWrapper.getChaosLevel * 100).toInt}%")
    
    // Call your actual AstraUploader.processBatch method
    uploader.processBatch(elements, topic, successfulBatch)
    
    val statsAfter = chaosWrapper.getStats
    val newChaosInjections = statsAfter.chaosInjections - statsBefore.chaosInjections
    
    if (newChaosInjections > 0) {
      println(s"🐒 Chaos injected $newChaosInjections failures during this batch")
    } else {
      println(s"✅ No chaos injections - batch should complete normally")
    }
  }
  
  def countRecords(session: com.datastax.oss.driver.api.core.CqlSession): Long = {
    try {
      val totalQuery = SimpleStatement.newInstance("SELECT COUNT(*) as total FROM simple_verification_test")
      val totalResult = session.execute(totalQuery).one()
      totalResult.getLong("total")
    } catch {
      case ex: Exception =>
        println(s"❌ Count query failed: ${ex.getMessage}")
        0L
    }
  }
  
  def showSampleRecords(session: com.datastax.oss.driver.api.core.CqlSession): Unit = {
    try {
      val sampleQuery = SimpleStatement.newInstance("SELECT message_data, topic FROM simple_verification_test LIMIT 10")
      val samples = session.execute(sampleQuery)
      
      println("📝 Sample records:")
      samples.forEach { row =>
        println(s"   • ${row.getString("message_data")} (topic: ${row.getString("topic")})")
      }
    } catch {
      case ex: Exception =>
        println(s"❌ Sample query failed: ${ex.getMessage}")
    }
  }
}