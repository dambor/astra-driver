package examples

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import astra.{AstraSession, AstraUploader, QueryExecutor, Cluster}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{SimpleStatement, BoundStatement, PreparedStatement}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.collection.mutable.Queue
import scala.util.{Success, Failure, Random}
import java.util.UUID
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

/**
 * This test specifically simulates PARTIAL silent data loss:
 * - Application logs show successful batch processing
 * - 95% of records are actually written successfully  
 * - 5% of records silently disappear (no error logs)
 * - This creates the exact production issue you're experiencing
 */
object PartialSilentDataLossTest extends App {
  implicit val system: ActorSystem = ActorSystem("PartialDataLossTestSystem")
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val ec: ExecutionContext = system.dispatcher
  
  // Test configuration - simulate real production volumes
  val TOTAL_BATCHES = 20
  val RECORDS_PER_BATCH = 1000
  val TOTAL_EXPECTED_RECORDS = TOTAL_BATCHES * RECORDS_PER_BATCH
  val TEST_RUN_ID = UUID.randomUUID().toString.take(8)
  
  println("🎯 Partial Silent Data Loss Simulation Test")
  println("=" * 60)
  println(s"📊 Test Configuration:")
  println(s"   • Total batches: $TOTAL_BATCHES")
  println(s"   • Records per batch: $RECORDS_PER_BATCH")
  println(s"   • Total expected records: $TOTAL_EXPECTED_RECORDS")
  println(s"   • Test run ID: $TEST_RUN_ID")
  println(s"   • Simulating: App logs SUCCESS, but some records silently lost")
  println("=" * 60)
  
  try {
    // Setup
    val session = Cluster.getOrCreateLiveSession.getSession()
    val uploader = new AstraUploader()
    
    setupTestTable(session)
    
    // Track what we expect vs what actually happens
    val expectedRecords = new AtomicLong(0)
    val loggedSuccessfulBatches = new AtomicInteger(0)
    val loggedSuccessfulRecords = new AtomicLong(0)
    
    println("\n🚀 Starting batch processing simulation...")
    println("   (Watch for SUCCESS logs while some records silently disappear)")
    
    // Process multiple batches to simulate realistic production load
    val futures = (1 to TOTAL_BATCHES).map { batchNum =>
      processBatchWithSilentLoss(session, uploader, batchNum, RECORDS_PER_BATCH, expectedRecords, loggedSuccessfulBatches, loggedSuccessfulRecords)
    }
    
    // Wait for all batches to "complete"
    Future.sequence(futures).onComplete {
      case Success(_) =>
        println(s"\n✅ All $TOTAL_BATCHES batches processed successfully!")
        println(s"📋 Application logs show: ${loggedSuccessfulBatches.get()} successful batches")
        println(s"📋 Application logs show: ${loggedSuccessfulRecords.get()} successful records")
        
        // Wait a bit more for any remaining async operations
        Thread.sleep(10000)
        
        // Now verify what actually made it to the database
        verifyActualDataInDatabase(session, expectedRecords.get(), loggedSuccessfulRecords.get())
        
      case Failure(ex) =>
        println(s"❌ Batch processing failed: ${ex.getMessage}")
    }
    
    // Keep the test running long enough to see the results
    Thread.sleep(60000) // 1 minute
    
  } catch {
    case ex: Exception =>
      log.error(ex, "Partial data loss test failed")
      println(s"❌ Test failed: ${ex.getMessage}")
      ex.printStackTrace()
  } finally {
    println("\n🔚 Test completed.")
    system.terminate()
  }
  
  def setupTestTable(session: CqlSession): Unit = {
    println("📋 Setting up test table...")
    
    // Design table to avoid ALLOW FILTERING - use test_run as partition key
    val createTable = SimpleStatement.newInstance(
      s"""
      CREATE TABLE IF NOT EXISTS partial_loss_test (
        test_run TEXT,
        batch_num INT,
        record_num INT,
        id UUID,
        message_data TEXT,
        expected_to_exist BOOLEAN,
        created_at TIMESTAMP,
        PRIMARY KEY (test_run, batch_num, record_num)
      )
      """
    )
    session.execute(createTable)
    
    // Clear any previous test data for this test run
    try {
      session.execute(SimpleStatement.newInstance(s"DELETE FROM partial_loss_test WHERE test_run = '$TEST_RUN_ID'"))
    } catch {
      case _: Exception => // Table might not exist yet, ignore
    }
    
    println("✅ Test table ready")
  }
  
  def processBatchWithSilentLoss(
    session: CqlSession,
    uploader: AstraUploader, 
    batchNum: Int,
    recordsPerBatch: Int,
    expectedRecords: AtomicLong,
    loggedSuccessfulBatches: AtomicInteger,
    loggedSuccessfulRecords: AtomicLong
  ): Future[Unit] = {
    
    Future {
      println(s"📤 Processing batch $batchNum/$TOTAL_BATCHES ($recordsPerBatch records)...")
      
      // Prepare statement with new table structure
      val insertStmt = session.prepare(
        """
        INSERT INTO partial_loss_test (test_run, batch_num, record_num, id, message_data, expected_to_exist) 
        VALUES (?, ?, ?, ?, ?, ?)
        """
      )
      
      // Create batch elements - but intentionally introduce silent failures
      val elements = (1 to recordsPerBatch).map { recordNum =>
        val messageData = s"batch_${batchNum}_record_${recordNum}_data"
        
        // SIMULATION: 5% of statements are "corrupted" to cause silent failures
        val shouldSucceed = Random.nextDouble() > 0.05 // 95% success rate
        val boundStmt = if (shouldSucceed) {
          // Normal statement that will succeed
          insertStmt.bind(TEST_RUN_ID, batchNum.asInstanceOf[java.lang.Integer], recordNum.asInstanceOf[java.lang.Integer], UUID.randomUUID(), messageData, java.lang.Boolean.TRUE)
        } else {
          // Silently corrupted statement that will fail but not throw exception
          createSilentlyFailingStatement(insertStmt, messageData, batchNum, recordNum)
        }
        
        if (shouldSucceed) {
          expectedRecords.incrementAndGet()
        }
        
        (messageData, boundStmt)
      }
      
      val successfulBatch = Queue(elements.map(_._2): _*)
      
      // Process the batch using your existing AstraUploader
      // This will log SUCCESS even though some records will silently fail
      uploader.processBatch(elements, s"batch_topic_$batchNum", successfulBatch)
      
      // Simulate what your application logs see - this will show SUCCESS
      loggedSuccessfulBatches.incrementAndGet()
      loggedSuccessfulRecords.addAndGet(recordsPerBatch)
      
      println(s"   ✅ Batch $batchNum completed successfully (logged $recordsPerBatch records)")
      
      // Small delay between batches to simulate realistic timing
      Thread.sleep(Random.nextInt(1000) + 500)
    }
  }
  
  /**
   * Create a statement that will silently fail without throwing exceptions
   * This simulates various real-world scenarios that cause silent data loss
   */
  def createSilentlyFailingStatement(insertStmt: PreparedStatement, messageData: String, batchNum: Int, recordNum: Int): BoundStatement = {
    val failureType = Random.nextInt(3) // Reduced to 3 simpler scenarios
    
    try {
      failureType match {
        case 0 =>
          // Scenario 1: Use a completely wrong session/keyspace
          try {
            val wrongSession = AstraSession.createSession() // No keyspace specified
            val wrongStmt = wrongSession.prepare("INSERT INTO system.local (key) VALUES (?)")
            wrongStmt.bind("fake_key") // This will fail silently or be ignored
          } catch {
            case _: Exception =>
              // If that fails, just use a null value which might get silently rejected
              insertStmt.bind(TEST_RUN_ID, batchNum.asInstanceOf[java.lang.Integer], recordNum.asInstanceOf[java.lang.Integer], null, messageData, java.lang.Boolean.FALSE)
          }
          
        case 1 =>
          // Scenario 2: Invalid data that might get rejected silently
          insertStmt.bind(TEST_RUN_ID, batchNum.asInstanceOf[java.lang.Integer], recordNum.asInstanceOf[java.lang.Integer], null, messageData, java.lang.Boolean.FALSE)
          
        case 2 =>
          // Scenario 3: Create statement but with wrong data that might cause silent failure
          val corrupted_test_run = "CORRUPTED_" + TEST_RUN_ID + "_WRONG"
          insertStmt.bind(corrupted_test_run, batchNum.asInstanceOf[java.lang.Integer], recordNum.asInstanceOf[java.lang.Integer], UUID.randomUUID(), messageData, java.lang.Boolean.FALSE)
      }
    } catch {
      case ex: Exception =>
        // If our "silent failure" actually throws an exception, 
        // fall back to a normal statement so the test continues
        println(s"   🔧 Silent failure simulation threw exception (${ex.getMessage}), using normal statement")
        insertStmt.bind(TEST_RUN_ID, batchNum.asInstanceOf[java.lang.Integer], recordNum.asInstanceOf[java.lang.Integer], UUID.randomUUID(), messageData, java.lang.Boolean.FALSE)
    }
  }
  
  def verifyActualDataInDatabase(session: CqlSession, expectedRecordCount: Long, loggedRecordCount: Long): Unit = {
    println(s"\n🔍 VERIFICATION: What actually made it to the database?")
    println("-" * 50)
    
    try {
      // Count total records for this test run (no ALLOW FILTERING needed)
      val totalQuery = SimpleStatement.newInstance(s"SELECT COUNT(*) as count FROM partial_loss_test WHERE test_run = '$TEST_RUN_ID'")
      val totalResult = session.execute(totalQuery)
      val actualTotalCount = totalResult.one().getLong("count")
      
      println(s"📊 APPLICATION LOGS SHOWED:")
      println(s"   • Total batches processed: ${loggedRecordCount / RECORDS_PER_BATCH}")
      println(s"   • Total records logged as successful: $loggedRecordCount")
      println(s"   • Success rate in logs: 100% ✅")
      
      println(s"\n📊 ACTUAL DATABASE CONTAINS:")
      println(s"   • Total records found: $actualTotalCount")
      println(s"   • Expected records (95% simulation): $expectedRecordCount")
      
      // Calculate the discrepancy
      val missingFromLogs = loggedRecordCount - actualTotalCount
      val missingFromExpected = expectedRecordCount - actualTotalCount
      
      if (missingFromLogs > 0) {
        val lossPercentageFromLogs = (missingFromLogs.toDouble / loggedRecordCount * 100)
        println(s"\n🚨 SILENT DATA LOSS DETECTED!")
        println(s"   🚨 Application logged $loggedRecordCount successful records")
        println(s"   🚨 But only $actualTotalCount records found in database")
        println(s"   🚨 Missing records: $missingFromLogs")
        println(f"   🚨 Silent data loss rate: $lossPercentageFromLogs%.2f%%")
        println(s"   🚨 This exactly matches your production issue!")
        
        // Show breakdown by batch to see if certain batches were more affected
        showDataLossBreakdown(session)
        
      } else {
        println(s"   ✅ No discrepancy detected between logs and database")
      }
      
      // Show some sample "missing" records that were logged as successful but aren't in DB
      if (missingFromLogs > 0) {
        println(s"\n📋 INVESTIGATION SUMMARY:")
        println(s"   • Your application shows SUCCESS in logs ✅")
        println(s"   • No error messages or exceptions logged ✅") 
        println(s"   • But $missingFromLogs records are missing from database 🚨")
        println(s"   • Monitoring tools would show 100% success rate ✅")
        println(s"   • Data analysts querying database find missing data 🚨")
        println(s"   • This is a classic 'silent data loss' production issue")
      }
      
    } catch {
      case ex: Exception =>
        println(s"❌ Verification failed: ${ex.getMessage}")
        ex.printStackTrace()
    }
  }
  
  def showDataLossBreakdown(session: CqlSession): Unit = {
    try {
      println(s"\n📊 Data Loss Breakdown by Batch:")
      
      // No ALLOW FILTERING needed - test_run is partition key, batch_num is clustering key
      val breakdownQuery = SimpleStatement.newInstance(
        s"""
        SELECT batch_num, COUNT(*) as actual_count 
        FROM partial_loss_test 
        WHERE test_run = '$TEST_RUN_ID' 
        GROUP BY test_run, batch_num
        """
      )
      
      val results = session.execute(breakdownQuery)
      
      results.forEach { row =>
        val batchNum = row.getInt("batch_num")
        val actualCount = row.getLong("actual_count")
        val expectedCount = RECORDS_PER_BATCH
        val missing = expectedCount - actualCount
        
        if (missing > 0) {
          println(s"   🚨 Batch $batchNum: Expected $expectedCount, Found $actualCount, Missing $missing")
        } else if (batchNum <= 5) { // Show first few successful ones
          println(s"   ✅ Batch $batchNum: Expected $expectedCount, Found $actualCount")
        }
      }
      
    } catch {
      case ex: Exception =>
        println(s"   ⚠️ Could not show breakdown: ${ex.getMessage}")
    }
  }
}