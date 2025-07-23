package examples

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import astra.{AstraSession, AstraUploader}
import com.datastax.oss.driver.api.core.cql.{SimpleStatement, BoundStatement}

import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import scala.collection.mutable.Queue
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}

object AstraUploaderChaosTest extends App {
  implicit val system: ActorSystem = ActorSystem("AstraUploaderChaosTestSystem")
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val ec: ExecutionContext = system.dispatcher
  
  println("🐒🚀 Testing Real AstraUploader with Chaos - ASYNC Testing with 1000 Records...")
  
  try {
    // Create session with chaos wrapper  
    val (realSession, chaosWrapper) = AstraSession.createSessionWithChaos("tradingtech")
    println("✅ Chaos session created!")
    
    // Create test table
    val createTable = SimpleStatement.newInstance(
      """
      CREATE TABLE IF NOT EXISTS uploader_chaos_test (
        id UUID PRIMARY KEY,
        batch_id TEXT,
        message_data TEXT,
        topic TEXT,
        test_phase TEXT,
        created_at TIMESTAMP
      )
      """
    )
    realSession.execute(createTable)
    println("✅ Table created!")
    
    // Add test_phase column if it doesn't exist (for existing tables)
    try {
      val alterTable = SimpleStatement.newInstance(
        "ALTER TABLE uploader_chaos_test ADD test_phase TEXT"
      )
      realSession.execute(alterTable)
      println("✅ Added test_phase column to existing table")
    } catch {
      case ex: Exception if ex.getMessage.contains("already exists") || ex.getMessage.contains("Invalid column name") =>
        println("✅ test_phase column already exists")
      case ex: Exception =>
        println(s"⚠️  Could not add test_phase column: ${ex.getMessage}")
        // Continue anyway - we'll just not use this column for tracking
    }
    
    // TRUNCATE TABLE BEFORE TESTING
    println("🧹 Truncating test table to ensure clean state...")
    val truncateTable = SimpleStatement.newInstance("TRUNCATE uploader_chaos_test")
    realSession.execute(truncateTable)
    println("✅ Table truncated - starting with clean slate!")
    
    // Verify table is empty
    val countQuery = SimpleStatement.newInstance("SELECT COUNT(*) as count FROM uploader_chaos_test")
    val countResult = realSession.execute(countQuery).one()
    val initialCount = countResult.getLong("count")
    println(s"📊 Initial record count: $initialCount (should be 0)")
    
    if (initialCount != 0) {
      println("⚠️  Warning: Table not properly truncated!")
    }
    
    // Prepare insert statement - handle case where test_phase column might not exist
    val insertStmt = try {
      realSession.prepare(
        """
        INSERT INTO uploader_chaos_test (id, batch_id, message_data, topic, test_phase, created_at) 
        VALUES (?, ?, ?, ?, ?, toTimestamp(now()))
        """
      )
    } catch {
      case ex: Exception if ex.getMessage.contains("test_phase") =>
        println("⚠️  test_phase column not available - using simplified insert")
        realSession.prepare(
          """
          INSERT INTO uploader_chaos_test (id, batch_id, message_data, topic, created_at) 
          VALUES (?, ?, ?, ?, toTimestamp(now()))
          """
        )
    }
    
    // Create your actual AstraUploader
    val uploader = new AstraUploader()
    
    // Test 1: Normal operation (no chaos) - 1000 records
    println("\n=== Test 1: AstraUploader Normal Operation (1000 records) ===")
    chaosWrapper.disableChaos()
    val normalFuture = testAstraUploaderBatchAsync(uploader, realSession, chaosWrapper, insertStmt, "Normal", "normal_topic", 1000)
    
    // Test 2: Low chaos - 1000 records
    println("\n=== Test 2: AstraUploader with 20% Chaos (1000 records) ===")
    chaosWrapper.enableChaos()
    chaosWrapper.setChaosLevel(0.2)
    val lowChaosFuture = testAstraUploaderBatchAsync(uploader, realSession, chaosWrapper, insertStmt, "LowChaos", "low_chaos_topic", 1000)
    
    // Test 3: Medium chaos - 1000 records
    println("\n=== Test 3: AstraUploader with 40% Chaos (1000 records) ===")
    chaosWrapper.setChaosLevel(0.4)
    val mediumChaosFuture = testAstraUploaderBatchAsync(uploader, realSession, chaosWrapper, insertStmt, "MediumChaos", "medium_chaos_topic", 1000)
    
    // Test 4: High chaos - 1000 records (this should really stress test it!)
    println("\n=== Test 4: AstraUploader with 60% Chaos (1000 records) ===")
    chaosWrapper.setChaosLevel(0.6)
    val highChaosFuture = testAstraUploaderBatchAsync(uploader, realSession, chaosWrapper, insertStmt, "HighChaos", "high_chaos_topic", 1000)
    
    // Wait for ALL async operations to complete
    println("\n⏳ Waiting for all async operations to complete...")
    val allFutures = Future.sequence(Seq(normalFuture, lowChaosFuture, mediumChaosFuture, highChaosFuture))
    
    try {
      Await.result(allFutures, 5.minutes)  // Give plenty of time for retries
      println("✅ All async operations completed!")
    } catch {
      case _: java.util.concurrent.TimeoutException =>
        println("⚠️  Timeout waiting for operations - some may still be running")
      case ex: Exception =>
        println(s"❌ Error waiting for operations: ${ex.getMessage}")
    }
    
    // Additional wait to ensure all background processing is done
    println("⏳ Additional wait for background processing...")
    Thread.sleep(30000)  // 30 seconds for any final async work
    
    // Verification - count what actually got inserted
    println("\n=== Verification: What Records Actually Exist? ===")
    verifyRecords(realSession)
    
    // Show final chaos statistics
    println("\n=== Final Chaos Statistics ===")
    println(chaosWrapper.getStats)
    
    realSession.close()
    
  } catch {
    case ex: Exception =>
      log.error(ex, "AstraUploader chaos test failed")
      println(s"❌ Test failed: ${ex.getMessage}")
      ex.printStackTrace()
  } finally {
    println("🔚 Terminating actor system...")
    system.terminate()
  }
  
  def testAstraUploaderBatchAsync(uploader: AstraUploader,
                                 realSession: com.datastax.oss.driver.api.core.CqlSession,
                                 chaosWrapper: astra.ChaosWrapper,
                                 insertStmt: com.datastax.oss.driver.api.core.cql.PreparedStatement,
                                 testName: String,
                                 topic: String,
                                 recordCount: Int): Future[Unit] = {
    
    Future {
      println(s"🧪 Testing AstraUploader '$testName' with $recordCount records on topic '$topic'...")
      
      val batchId = UUID.randomUUID().toString
      val startTime = System.currentTimeMillis()
      
      // Create elements in the format AstraUploader expects: Seq[(Any, BoundStatement)]
      val elements = (1 to recordCount).map { i =>
        val messageData = s"${testName}_message_$i"
        val boundStmt = try {
          // Try with test_phase column first
          insertStmt.bind(
            UUID.randomUUID(),
            batchId,
            messageData,
            topic,
            testName  // test_phase column
          )
        } catch {
          case _: Exception =>
            // Fall back to insert without test_phase column
            insertStmt.bind(
              UUID.randomUUID(),
              batchId,
              messageData,
              topic
            )
        }
        (messageData, boundStmt)  // (Any, BoundStatement)
      }
      
      // Create successfulBatch in the format AstraUploader expects: Queue[BoundStatement]
      val successfulBatch = Queue(elements.map(_._2): _*)
      
      val statsBefore = chaosWrapper.getStats
      
      println(s"📤 Calling AstraUploader.processBatch for batch '$testName'...")
      println(s"   • Batch ID: $batchId")
      println(s"   • Records: $recordCount")
      println(s"   • Topic: $topic")
      println(s"   • Chaos Level: ${(chaosWrapper.getChaosLevel * 100).toInt}%")
      println(s"   • Thread: ${Thread.currentThread().getName}")
      
      // Call AstraUploader.processBatch (returns Unit but may trigger async operations)
      try {
        println(s"🔄 Calling processBatch (synchronous call that may trigger async work)...")
        uploader.processBatch(elements, topic, successfulBatch)
        println(s"✅ processBatch call completed for $testName")
        
        // Even though processBatch returns Unit, it may have triggered async operations
        // We need to wait for those async operations to complete
        println(s"⏳ Waiting for async operations triggered by processBatch...")
        
      } catch {
        case ex: Exception =>
          println(s"❌ processBatch failed for $testName: ${ex.getMessage}")
          throw ex
      }
      
      val endTime = System.currentTimeMillis()
      val duration = endTime - startTime
      val statsAfter = chaosWrapper.getStats
      val newChaosInjections = statsAfter.chaosInjections - statsBefore.chaosInjections
      
      println(s"⏱️  Batch '$testName' completed in ${duration}ms")
      
      if (newChaosInjections > 0) {
        println(s"🐒 Chaos injected $newChaosInjections failures during this batch")
        println(s"🔍 Retry behavior should have handled these failures...")
      } else {
        println(s"✅ No chaos injections for this batch - should complete normally")
      }
      
      // Wait a bit more to ensure any async callbacks complete
      Thread.sleep(5000)
      println(s"🏁 Batch '$testName' fully processed")
    }
  }
  
  def verifyRecords(session: com.datastax.oss.driver.api.core.CqlSession): Unit = {
    try {
      // Simple count query (avoid GROUP BY issues)
      val totalQuery = SimpleStatement.newInstance("SELECT COUNT(*) as total FROM uploader_chaos_test")
      val totalResult = session.execute(totalQuery).one()
      val totalCount = totalResult.getLong("total")
      
      println(s"📊 TOTAL RECORDS in database: $totalCount")
      
      // Expected total: 1000 (normal) + 1000 (low chaos) + 1000 (medium chaos) + 1000 (high chaos) = 4000 records
      val expectedTotal = 4000
      
      if (totalCount == expectedTotal) {
        println(s"✅ SUCCESS: Found expected $expectedTotal records")
      } else if (totalCount < expectedTotal) {
        println(s"🚨 MISSING RECORDS: Expected $expectedTotal, found $totalCount")
        println(s"🚨 Missing ${expectedTotal - totalCount} records - THIS IS THE BUG!")
        val lossPercentage = ((expectedTotal - totalCount).toDouble / expectedTotal * 100).formatted("%.2f")
        println(s"🚨 Data loss: $lossPercentage%")
      } else {
        println(s"🔄 DUPLICATE RECORDS: Expected $expectedTotal, found $totalCount") 
        println(s"🔄 Extra ${totalCount - expectedTotal} records - retry logic issue?")
      }
      
      // Show breakdown by test phase
      println("\n📋 Records by test phase:")
      
      try {
        val phases = Seq("Normal", "LowChaos", "MediumChaos", "HighChaos")
        phases.foreach { phase =>
          try {
            val phaseQuery = SimpleStatement.newInstance(s"SELECT COUNT(*) as count FROM uploader_chaos_test WHERE test_phase = '$phase'")
            val phaseResult = session.execute(phaseQuery).one()
            val phaseCount = phaseResult.getLong("count")
            val expected = 1000
            val status = if (phaseCount == expected) "✅" else if (phaseCount < expected) "🚨" else "🔄"
            println(s"   $status $phase: $phaseCount/$expected records")
            
            if (phaseCount != expected) {
              val diff = expected - phaseCount
              if (diff > 0) {
                println(s"      ⚠️  Missing $diff records in $phase phase")
              } else {
                println(s"      ⚠️  Extra ${-diff} records in $phase phase")
              }
            }
          } catch {
            case ex: Exception if ex.getMessage.contains("test_phase") =>
              // test_phase column doesn't exist, try to count by message_data pattern
              val phaseQuery = SimpleStatement.newInstance(s"SELECT COUNT(*) as count FROM uploader_chaos_test WHERE message_data LIKE '${phase}_%' ALLOW FILTERING")
              val phaseResult = session.execute(phaseQuery).one()
              val phaseCount = phaseResult.getLong("count")
              val expected = 1000
              val status = if (phaseCount == expected) "✅" else if (phaseCount < expected) "🚨" else "🔄"
              println(s"   $status $phase: $phaseCount/$expected records (via pattern matching)")
          }
        }
      } catch {
        case ex: Exception => 
          println(s"   ❌ Could not get phase breakdown: ${ex.getMessage}")
          println("   📊 Using topic-based breakdown instead...")
          
          // Try counting by topic as fallback
          try {
            val topics = Seq("normal_topic", "low_chaos_topic", "medium_chaos_topic", "high_chaos_topic")
            topics.foreach { topic =>
              val topicQuery = SimpleStatement.newInstance(s"SELECT COUNT(*) as count FROM uploader_chaos_test WHERE topic = '$topic'")
              val topicResult = session.execute(topicQuery).one()
              val topicCount = topicResult.getLong("count")
              println(s"   📍 $topic: $topicCount records")
            }
          } catch {
            case _: Exception =>
              println("   ❌ Could not get breakdown by topic either")
          }
      }
      
      // Show some sample data to verify structure
      val sampleQuery = SimpleStatement.newInstance("SELECT batch_id, message_data, topic FROM uploader_chaos_test LIMIT 20")
      val samples = session.execute(sampleQuery)
      
      println("\n📝 Sample records:")
      var sampleCount = 0
      samples.forEach { row =>
        if (sampleCount < 10) {  // Limit console output
          val messageData = row.getString("message_data")
          val topic = row.getString("topic")
          val testPhase = messageData.split("_")(0)  // Extract phase from message_data
          println(s"   • $messageData ($testPhase, topic: $topic)")
          sampleCount += 1
        }
      }
      
    } catch {
      case ex: Exception =>
        println(s"❌ Verification failed: ${ex.getMessage}")
        ex.printStackTrace()
    }
  }
}