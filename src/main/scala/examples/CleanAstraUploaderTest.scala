package examples

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import astra.{AstraSession, AstraUploader, Cluster}
import com.datastax.oss.driver.api.core.cql.{SimpleStatement, BoundStatement}

import scala.concurrent.ExecutionContext
import scala.collection.mutable.Queue
import java.util.UUID

object CleanAstraUploaderTest extends App {
  implicit val system: ActorSystem = ActorSystem("CleanAstraUploaderTestSystem")
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val ec: ExecutionContext = system.dispatcher
  
  println("🐒🧹 Clean AstraUploader Test - Fresh Start...")
  
  try {
    // Create session with chaos wrapper  
    val (realSession, chaosWrapper) = AstraSession.createSessionWithChaos("tradingtech")
    println("✅ Chaos session created!")
    
    // 🧹 CLEAR the table first
    println("🧹 Clearing test table for fresh start...")
    val dropTable = SimpleStatement.newInstance("DROP TABLE IF EXISTS uploader_chaos_test")
    realSession.execute(dropTable)
    println("✅ Old table dropped!")
    
    // Create test table
    val createTable = SimpleStatement.newInstance(
      """
      CREATE TABLE IF NOT EXISTS uploader_chaos_test (
        id UUID PRIMARY KEY,
        batch_id TEXT,
        message_data TEXT,
        topic TEXT,
        test_run TEXT,
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
      INSERT INTO uploader_chaos_test (id, batch_id, message_data, topic, test_run, created_at) 
      VALUES (?, ?, ?, ?, ?, toTimestamp(now()))
      """
    )
    println("✅ Statement prepared on cluster session!")
    
    // Create your actual AstraUploader
    val uploader = new AstraUploader()
    val testRunId = UUID.randomUUID().toString.take(8)  // Unique identifier for this test run
    
    // Test 1: Normal operation (no chaos)
    println(s"\n=== Test 1: Normal Operation (Test Run: $testRunId) ===")
    chaosWrapper.disableChaos()
    testAstraUploaderBatch(uploader, clusterSession, chaosWrapper, insertStmt, "Normal", "normal_topic", 5, testRunId)
    
    Thread.sleep(5000)  // Wait for async completion
    
    // Test 2: Low chaos 
    println(s"\n=== Test 2: 20% Chaos (Test Run: $testRunId) ===")
    chaosWrapper.enableChaos()
    chaosWrapper.setChaosLevel(0.2)
    testAstraUploaderBatch(uploader, clusterSession, chaosWrapper, insertStmt, "LowChaos", "low_chaos_topic", 10, testRunId)
    
    Thread.sleep(10000)  // Wait longer for retries
    
    // Test 3: Medium chaos 
    println(s"\n=== Test 3: 40% Chaos (Test Run: $testRunId) ===")
    chaosWrapper.setChaosLevel(0.4)
    testAstraUploaderBatch(uploader, clusterSession, chaosWrapper, insertStmt, "MediumChaos", "medium_chaos_topic", 15, testRunId)
    
    Thread.sleep(15000)  // Wait even longer for retries
    
    // Verification - count what actually got inserted for THIS test run
    println(s"\n=== Verification: Records for Test Run $testRunId ===")
    verifyRecords(clusterSession, testRunId)
    
    // Show final chaos statistics
    println("\n=== Final Chaos Statistics ===")
    println(chaosWrapper.getStats)
    
    clusterSession.close()
    
  } catch {
    case ex: Exception =>
      log.error(ex, "Clean AstraUploader test failed")
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
                            recordCount: Int,
                            testRunId: String): Unit = {
    
    println(s"🧪 Testing AstraUploader '$testName' with $recordCount records on topic '$topic'...")
    
    val batchId = UUID.randomUUID().toString
    
    // Create elements in the format AstraUploader expects: Seq[(Any, BoundStatement)]
    val elements = (1 to recordCount).map { i =>
      val messageData = s"${testName}_message_$i"
      val boundStmt = insertStmt.bind(
        UUID.randomUUID(),
        batchId,
        messageData,
        topic,
        testRunId  // Add test run ID to track this specific test
      )
      (messageData, boundStmt)  // (Any, BoundStatement)
    }
    
    // Create successfulBatch in the format AstraUploader expects: Queue[BoundStatement]
    val successfulBatch = Queue(elements.map(_._2): _*)
    
    val statsBefore = chaosWrapper.getStats
    
    println(s"📤 Calling AstraUploader.processBatch for batch '$testName'...")
    println(s"   • Batch ID: $batchId")
    println(s"   • Records: $recordCount")
    println(s"   • Topic: $topic")
    println(s"   • Test Run: $testRunId")
    println(s"   • Chaos Level: ${(chaosWrapper.getChaosLevel * 100).toInt}%")
    
    // Call your actual AstraUploader.processBatch method
    uploader.processBatch(elements, topic, successfulBatch)
    
    val statsAfter = chaosWrapper.getStats
    val newChaosInjections = statsAfter.chaosInjections - statsBefore.chaosInjections
    
    if (newChaosInjections > 0) {
      println(s"🐒 Chaos injected $newChaosInjections failures during this batch")
      println(s"🔍 Watch for retry behavior in the logs above...")
    } else {
      println(s"✅ No chaos injections for this batch - should complete normally")
    }
  }
  
  def verifyRecords(session: com.datastax.oss.driver.api.core.CqlSession, testRunId: String): Unit = {
    try {
      // Count records for this specific test run only
      val totalQuery = SimpleStatement.newInstance(
        "SELECT COUNT(*) as total FROM uploader_chaos_test WHERE test_run = ?",
        testRunId
      )
      val totalResult = session.execute(totalQuery).one()
      val totalCount = totalResult.getLong("total")
      
      println(s"📊 TOTAL RECORDS for this test run: $totalCount")
      
      // Expected total: 5 (normal) + 10 (low chaos) + 15 (medium chaos) = 30 records
      val expectedTotal = 5 + 10 + 15
      
      if (totalCount == expectedTotal) {
        println(s"✅ SUCCESS: Found expected $expectedTotal records - SESSION BUG COMPLETELY FIXED!")
        println(s"🎉 Your missing records issue is SOLVED!")
      } else if (totalCount < expectedTotal) {
        println(s"🚨 MISSING RECORDS: Expected $expectedTotal, found $totalCount")
        println(s"🚨 Missing ${expectedTotal - totalCount} records - there may be other issues")
      } else {
        println(s"🔄 DUPLICATE RECORDS: Expected $expectedTotal, found $totalCount") 
        println(s"🔄 Extra ${totalCount - expectedTotal} records - potential retry logic issue")
      }
      
      // Show breakdown by test phase
      val breakdownQuery = SimpleStatement.newInstance(
        "SELECT message_data FROM uploader_chaos_test WHERE test_run = ? LIMIT 20",
        testRunId
      )
      val samples = session.execute(breakdownQuery)
      
      println("📝 Sample records from this test run:")
      samples.forEach { row =>
        println(s"   • ${row.getString("message_data")}")
      }
      
    } catch {
      case ex: Exception =>
        println(s"❌ Verification failed: ${ex.getMessage}")
        ex.printStackTrace()
    }
  }
}