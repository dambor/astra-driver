package examples

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import astra.{AstraSession, AstraUploader, QueryExecutor, Cluster}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{SimpleStatement, BoundStatement, PreparedStatement}

import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import scala.collection.mutable.Queue
import scala.util.{Success, Failure, Random, Try}
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}

/**
 * This test simulates the exact production issue:
 * - Writer application logs show "successful uploads"
 * - But records are missing when queried through Astra CLI
 * 
 * Potential root causes this test investigates:
 * 1. Session consistency issues (different sessions for write/read)
 * 2. Failed async operations that don't propagate errors properly
 * 3. Transaction-like behavior where some records commit, others don't
 * 4. Race conditions between writes and reads
 * 5. Keyspace/table targeting mismatches
 */
object MissingRecordsSimulationTest extends App {
  implicit val system: ActorSystem = ActorSystem("MissingRecordsTestSystem")
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val ec: ExecutionContext = system.dispatcher
  
  // Test configuration
  val TEST_RECORDS_COUNT = 1000
  val BATCH_SIZE = 100
  val TEST_RUN_ID = UUID.randomUUID().toString.take(8)
  
  println("🚨 Missing Records Simulation Test")
  println("=" * 60)
  println(s"📊 Test Configuration:")
  println(s"   • Total records to insert: $TEST_RECORDS_COUNT")
  println(s"   • Batch size: $BATCH_SIZE")
  println(s"   • Test run ID: $TEST_RUN_ID")
  println(s"   • This test simulates your production issue")
  println("=" * 60)
  
  try {
    // Scenario 1: Different Session Types (Most likely cause)
    println("\n🎯 SCENARIO 1: Different Session Types")
    println("   Testing if different sessions see different data...")
    testDifferentSessionTypes()
    
    // Scenario 2: Failed Async Operations with False Success Logs
    println("\n🎯 SCENARIO 2: Failed Async Operations")
    println("   Testing if async failures are properly detected...")
    testFailedAsyncOperations()
    
    // Scenario 3: Session Reset During Operations
    println("\n🎯 SCENARIO 3: Session Reset Issues")
    println("   Testing if session resets cause data loss...")
    testSessionResetDuringOperations()
    
    // Scenario 4: Race Conditions
    println("\n🎯 SCENARIO 4: Race Conditions")
    println("   Testing read/write race conditions...")
    testRaceConditions()
    
    // Scenario 5: Keyspace Mismatch
    println("\n🎯 SCENARIO 5: Keyspace Targeting")
    println("   Testing if writes/reads target different keyspaces...")
    testKeyspaceMismatch()
    
    // Final comprehensive verification
    println("\n🔍 FINAL COMPREHENSIVE VERIFICATION")
    println("   Checking all possible locations for missing data...")
    comprehensiveDataVerification()
    
  } catch {
    case ex: Exception =>
      log.error(ex, "Missing records simulation test failed")
      println(s"❌ Test failed: ${ex.getMessage}")
      ex.printStackTrace()
  } finally {
    println("\n🔚 Test completed. Check results above.")
    system.terminate()
  }
  
  /**
   * SCENARIO 1: Different Session Types
   * Tests if using different session creation methods causes inconsistency
   */
  def testDifferentSessionTypes(): Unit = {
    println("   📝 Creating different types of sessions...")
    
    // Session Type 1: Cluster session (like your main app)
    val clusterSession = Cluster.getOrCreateLiveSession.getSession()
    
    // Session Type 2: Direct session with keyspace
    val directSession = AstraSession.createSession("tradingtech")
    
    // Session Type 3: Direct session without keyspace, then USE statement
    val genericSession = AstraSession.createSession()
    genericSession.execute(SimpleStatement.newInstance("USE tradingtech"))
    
    // Create test table on cluster session
    val createTable = SimpleStatement.newInstance(
      s"""
      CREATE TABLE IF NOT EXISTS missing_records_test_scenario1 (
        id UUID PRIMARY KEY,
        test_run TEXT,
        session_type TEXT,
        message_data TEXT,
        writer_timestamp BIGINT,
        created_at TIMESTAMP
      )
      """
    )
    clusterSession.execute(createTable)
    directSession.execute(createTable)
    genericSession.execute(createTable)
    
    // Clear any existing data
    clusterSession.execute(SimpleStatement.newInstance("TRUNCATE missing_records_test_scenario1"))
    
    println(s"   📤 Writing $BATCH_SIZE records using CLUSTER session...")
    writeRecordsWithSession(clusterSession, "cluster", BATCH_SIZE, "scenario1")
    
    println(s"   📤 Writing $BATCH_SIZE records using DIRECT session...")
    writeRecordsWithSession(directSession, "direct", BATCH_SIZE, "scenario1")
    
    println(s"   📤 Writing $BATCH_SIZE records using GENERIC session...")
    writeRecordsWithSession(genericSession, "generic", BATCH_SIZE, "scenario1")
    
    Thread.sleep(5000) // Wait for async operations
    
    // Now read from each session type
    println("   🔍 Reading from each session type...")
    val clusterCount = countRecordsFromSession(clusterSession, "scenario1")
    val directCount = countRecordsFromSession(directSession, "scenario1")
    val genericCount = countRecordsFromSession(genericSession, "scenario1")
    
    println(s"   📊 CLUSTER session sees: $clusterCount records")
    println(s"   📊 DIRECT session sees: $directCount records")
    println(s"   📊 GENERIC session sees: $genericCount records")
    
    val expectedTotal = BATCH_SIZE * 3
    if (clusterCount != expectedTotal || directCount != expectedTotal || genericCount != expectedTotal) {
      println(s"   🚨 SESSION INCONSISTENCY DETECTED!")
      println(s"   🚨 Expected $expectedTotal records in each session")
      println(s"   🚨 This could explain missing records in production!")
      
      // Show detailed breakdown
      showRecordBreakdownBySession(clusterSession, "scenario1")
    } else {
      println(s"   ✅ All sessions see the same data ($expectedTotal records)")
    }
    
    // Cleanup
    directSession.close()
    genericSession.close()
  }
  
  /**
   * SCENARIO 2: Failed Async Operations
   * Tests if async operations fail silently while logging success
   */
  def testFailedAsyncOperations(): Unit = {
    println("   📝 Testing async operation failure detection...")
    
    val session = Cluster.getOrCreateLiveSession.getSession()
    val uploader = new AstraUploader()
    
    // Create test table
    val createTable = SimpleStatement.newInstance(
      s"""
      CREATE TABLE IF NOT EXISTS missing_records_test_scenario2 (
        id UUID PRIMARY KEY,
        test_run TEXT,
        message_data TEXT,
        batch_id TEXT,
        created_at TIMESTAMP
      )
      """
    )
    session.execute(createTable)
    session.execute(SimpleStatement.newInstance("TRUNCATE missing_records_test_scenario2"))
    
    val insertStmt = session.prepare(
      "INSERT INTO missing_records_test_scenario2 (id, test_run, message_data, batch_id, created_at) VALUES (?, ?, ?, ?, toTimestamp(now()))"
    )
    
    val loggedSuccesses = new AtomicInteger(0)
    val actualSuccesses = new AtomicInteger(0)
    
    // Override the AstraUploader behavior to simulate logging without actual success
    println(s"   📤 Attempting to insert $BATCH_SIZE records with potential async failures...")
    
    val elements = (1 to BATCH_SIZE).map { i =>
      val messageData = s"async_test_message_$i"
      val boundStmt = insertStmt.bind(UUID.randomUUID(), TEST_RUN_ID, messageData, "batch_async_test")
      (messageData, boundStmt)
    }
    
    val successfulBatch = Queue(elements.map(_._2): _*)
    
    // Simulate what your app does - processBatch call
    val startTime = System.currentTimeMillis()
    uploader.processBatch(elements, "async_test_topic", successfulBatch)
    
    // Your application would log success here, but let's check if data actually made it
    loggedSuccesses.addAndGet(BATCH_SIZE)
    println(s"   📋 Application logged: ${loggedSuccesses.get()} successful uploads")
    
    // Wait for async operations
    Thread.sleep(10000)
    
    // Check what actually made it to the database
    val actualCount = countRecordsFromSession(session, "scenario2", Some("batch_async_test"))
    actualSuccesses.set(actualCount.toInt)
    
    println(s"   📊 Application logged successful: ${loggedSuccesses.get()} records")
    println(s"   📊 Database actually contains: ${actualSuccesses.get()} records")
    
    if (loggedSuccesses.get() != actualSuccesses.get()) {
      println(s"   🚨 ASYNC FAILURE DETECTION ISSUE!")
      println(s"   🚨 Missing ${loggedSuccesses.get() - actualSuccesses.get()} records")
      println(s"   🚨 Your app logs success but async operations failed!")
    } else {
      println(s"   ✅ Logged successes match actual database records")
    }
  }
  
  /**
   * SCENARIO 3: Session Reset During Operations
   * Tests if session resets cause data loss
   */
  def testSessionResetDuringOperations(): Unit = {
    println("   📝 Testing session reset during operations...")
    
    val session = Cluster.getOrCreateLiveSession.getSession()
    val uploader = new AstraUploader()
    
    // Create test table
    val createTable = SimpleStatement.newInstance(
      s"""
      CREATE TABLE IF NOT EXISTS missing_records_test_scenario3 (
        id UUID PRIMARY KEY,
        test_run TEXT,
        message_data TEXT,
        phase TEXT,
        created_at TIMESTAMP
      )
      """
    )
    session.execute(createTable)
    session.execute(SimpleStatement.newInstance("TRUNCATE missing_records_test_scenario3"))
    
    val insertStmt = session.prepare(
      "INSERT INTO missing_records_test_scenario3 (id, test_run, message_data, phase, created_at) VALUES (?, ?, ?, ?, toTimestamp(now()))"
    )
    
    // Phase 1: Insert some records normally
    println(s"   📤 Phase 1: Inserting ${BATCH_SIZE/2} records normally...")
    val phase1Elements = (1 to BATCH_SIZE/2).map { i =>
      val messageData = s"phase1_message_$i"
      val boundStmt = insertStmt.bind(UUID.randomUUID(), TEST_RUN_ID, messageData, "phase1")
      (messageData, boundStmt)
    }
    uploader.processBatch(phase1Elements, "phase1_topic", Queue(phase1Elements.map(_._2): _*))
    Thread.sleep(3000)
    
    val phase1Count = countRecordsFromSession(session, "scenario3", Some("phase1"))
    println(s"   📊 Phase 1 completed: $phase1Count records in database")
    
    // Phase 2: Reset session in the middle of operations
    println(s"   🔄 Phase 2: Resetting session then inserting ${BATCH_SIZE/2} more records...")
    Cluster.getOrCreateLiveSession.resetSession()
    Thread.sleep(2000) // Wait for reset
    
    // Get new session after reset
    val newSession = Cluster.getOrCreateLiveSession.getSession()
    val newInsertStmt = newSession.prepare(
      "INSERT INTO missing_records_test_scenario3 (id, test_run, message_data, phase, created_at) VALUES (?, ?, ?, ?, toTimestamp(now()))"
    )
    
    val phase2Elements = (1 to BATCH_SIZE/2).map { i =>
      val messageData = s"phase2_message_$i"
      val boundStmt = newInsertStmt.bind(UUID.randomUUID(), TEST_RUN_ID, messageData, "phase2")
      (messageData, boundStmt)
    }
    uploader.processBatch(phase2Elements, "phase2_topic", Queue(phase2Elements.map(_._2): _*))
    Thread.sleep(5000)
    
    // Check final counts
    val phase2Count = countRecordsFromSession(newSession, "scenario3", Some("phase2"))
    val totalCount = countRecordsFromSession(newSession, "scenario3")
    
    println(s"   📊 Phase 2 completed: $phase2Count records in database")
    println(s"   📊 Total records: $totalCount (expected: $BATCH_SIZE)")
    
    if (totalCount < BATCH_SIZE) {
      println(s"   🚨 SESSION RESET CAUSED DATA LOSS!")
      println(s"   🚨 Missing ${BATCH_SIZE - totalCount} records after reset")
    } else {
      println(s"   ✅ Session reset handled correctly")
    }
  }
  
  /**
   * SCENARIO 4: Race Conditions
   * Tests if rapid read/write operations cause inconsistencies
   */
  def testRaceConditions(): Unit = {
    println("   📝 Testing read/write race conditions...")
    
    val session = Cluster.getOrCreateLiveSession.getSession()
    val uploader = new AstraUploader()
    
    // Create test table
    val createTable = SimpleStatement.newInstance(
      s"""
      CREATE TABLE IF NOT EXISTS missing_records_test_scenario4 (
        id UUID PRIMARY KEY,
        test_run TEXT,
        message_data TEXT,
        thread_id TEXT,
        created_at TIMESTAMP
      )
      """
    )
    session.execute(createTable)
    session.execute(SimpleStatement.newInstance("TRUNCATE missing_records_test_scenario4"))
    
    val insertStmt = session.prepare(
      "INSERT INTO missing_records_test_scenario4 (id, test_run, message_data, thread_id, created_at) VALUES (?, ?, ?, ?, toTimestamp(now()))"
    )
    
    val recordsWritten = new AtomicInteger(0)
    val recordsRead = new AtomicInteger(0)
    val isWriting = new AtomicBoolean(true)
    
    // Start background writer
    val writerFuture = Future {
      (1 to BATCH_SIZE).foreach { i =>
        try {
          val messageData = s"race_test_message_$i"
          val boundStmt = insertStmt.bind(UUID.randomUUID(), TEST_RUN_ID, messageData, "writer_thread")
          val elements = List((messageData, boundStmt))
          uploader.processBatch(elements, s"race_topic_$i", Queue(boundStmt))
          recordsWritten.incrementAndGet()
          Thread.sleep(50) // Small delay between writes
        } catch {
          case ex: Exception =>
            println(s"   ⚠️ Write $i failed: ${ex.getMessage}")
        }
      }
      isWriting.set(false)
    }
    
    // Start background reader
    val readerFuture = Future {
      while (isWriting.get() || recordsRead.get() < recordsWritten.get()) {
        try {
          val count = countRecordsFromSession(session, "scenario4")
          recordsRead.set(count.toInt)
          Thread.sleep(100) // Read every 100ms
        } catch {
          case ex: Exception =>
            println(s"   ⚠️ Read failed: ${ex.getMessage}")
        }
      }
    }
    
    // Wait for both to complete
    Await.ready(writerFuture, 30.seconds)
    Thread.sleep(5000) // Extra time for final writes to complete
    Await.ready(readerFuture, 10.seconds)
    
    val finalCount = countRecordsFromSession(session, "scenario4")
    
    println(s"   📊 Records written by writer: ${recordsWritten.get()}")
    println(s"   📊 Last count seen by reader: ${recordsRead.get()}")
    println(s"   📊 Final database count: $finalCount")
    
    if (finalCount < recordsWritten.get()) {
      println(s"   🚨 RACE CONDITION DETECTED!")
      println(s"   🚨 Writer reported ${recordsWritten.get()} writes but only $finalCount in database")
    } else {
      println(s"   ✅ No race condition issues detected")
    }
  }
  
  /**
   * SCENARIO 5: Keyspace Mismatch
   * Tests if writes and reads target different keyspaces
   */
  def testKeyspaceMismatch(): Unit = {
    println("   📝 Testing keyspace targeting issues...")
    
    // Create sessions targeting different keyspaces
    val primarySession = AstraSession.createSession("tradingtech")
    
    // Create table in primary keyspace
    val createTable = SimpleStatement.newInstance(
      s"""
      CREATE TABLE IF NOT EXISTS missing_records_test_scenario5 (
        id UUID PRIMARY KEY,
        test_run TEXT,
        message_data TEXT,
        target_keyspace TEXT,
        created_at TIMESTAMP
      )
      """
    )
    primarySession.execute(createTable)
    primarySession.execute(SimpleStatement.newInstance("TRUNCATE missing_records_test_scenario5"))
    
    val insertStmt = primarySession.prepare(
      "INSERT INTO missing_records_test_scenario5 (id, test_run, message_data, target_keyspace, created_at) VALUES (?, ?, ?, ?, toTimestamp(now()))"
    )
    
    println(s"   📤 Writing $BATCH_SIZE records to 'tradingtech' keyspace...")
    (1 to BATCH_SIZE).foreach { i =>
      val boundStmt = insertStmt.bind(UUID.randomUUID(), TEST_RUN_ID, s"keyspace_test_$i", "tradingtech")
      primarySession.execute(boundStmt)
    }
    
    // Now try to read from different session configurations
    val clusterSession = Cluster.getOrCreateLiveSession.getSession()
    val directSession = AstraSession.createSession("tradingtech")
    
    val primaryCount = countRecordsFromSession(primarySession, "scenario5")
    val clusterCount = countRecordsFromSession(clusterSession, "scenario5")
    val directCount = countRecordsFromSession(directSession, "scenario5")
    
    println(s"   📊 Primary session count: $primaryCount")
    println(s"   📊 Cluster session count: $clusterCount")
    println(s"   📊 Direct session count: $directCount")
    
    if (primaryCount != clusterCount || primaryCount != directCount) {
      println(s"   🚨 KEYSPACE TARGETING MISMATCH!")
      println(s"   🚨 Different sessions see different data!")
      
      // Show which keyspace each session is actually using
      showSessionKeyspaces(primarySession, "primary")
      showSessionKeyspaces(clusterSession, "cluster")
      showSessionKeyspaces(directSession, "direct")
    } else {
      println(s"   ✅ All sessions see consistent data")
    }
    
    primarySession.close()
    directSession.close()
  }
  
  /**
   * Comprehensive verification across all scenarios
   */
  def comprehensiveDataVerification(): Unit = {
    println("   📝 Performing comprehensive data verification...")
    
    val session = Cluster.getOrCreateLiveSession.getSession()
    
    val scenarios = List("scenario1", "scenario2", "scenario3", "scenario4", "scenario5")
    var totalFound = 0L
    var totalExpected = 0L
    
    scenarios.foreach { scenario =>
      try {
        val count = countRecordsFromSession(session, scenario)
        val expected = scenario match {
          case "scenario1" => BATCH_SIZE * 3  // 3 session types
          case "scenario2" => BATCH_SIZE      // 1 batch
          case "scenario3" => BATCH_SIZE      // 2 phases
          case "scenario4" => BATCH_SIZE      // race condition test
          case "scenario5" => BATCH_SIZE      // keyspace test
        }
        
        totalFound += count
        totalExpected += expected
        
        val status = if (count >= expected * 0.95) "✅" else "🚨"
        println(s"   $status $scenario: $count/$expected records")
        
        if (count < expected) {
          println(s"       ⚠️ Missing ${expected - count} records in $scenario")
        }
      } catch {
        case ex: Exception =>
          println(s"   ❌ $scenario: Error querying - ${ex.getMessage}")
      }
    }
    
    println(s"\n   📊 COMPREHENSIVE SUMMARY:")
    println(s"   📊 Total found across all tests: $totalFound")
    println(s"   📊 Total expected across all tests: $totalExpected")
    
    if (totalFound < totalExpected) {
      val missingCount = totalExpected - totalFound
      val lossPercentage = (missingCount.toDouble / totalExpected * 100)
      println(s"   🚨 MISSING RECORDS DETECTED!")
      println(s"   🚨 Missing $missingCount records (${lossPercentage.formatted("%.2f")}% data loss)")
      println(s"   🚨 This reproduces your production issue!")
    } else {
      println(s"   ✅ All expected records found")
    }
  }
  
  // Helper methods
  def writeRecordsWithSession(session: CqlSession, sessionType: String, count: Int, scenario: String): Unit = {
    val insertStmt = session.prepare(
      s"INSERT INTO missing_records_test_$scenario (id, test_run, session_type, message_data, writer_timestamp, created_at) VALUES (?, ?, ?, ?, ?, toTimestamp(now()))"
    )
    
    (1 to count).foreach { i =>
      val boundStmt = insertStmt.bind(
        UUID.randomUUID(), 
        TEST_RUN_ID, 
        sessionType, 
        s"${sessionType}_message_$i",
        System.currentTimeMillis().asInstanceOf[java.lang.Long]
      )
      session.execute(boundStmt)
    }
  }
  
  def countRecordsFromSession(session: CqlSession, scenario: String, filter: Option[String] = None): Long = {
    try {
      val query = filter match {
        case Some(filterValue) =>
          if (scenario == "scenario2") {
            SimpleStatement.newInstance(s"SELECT COUNT(*) as count FROM missing_records_test_$scenario WHERE batch_id = ?", filterValue)
          } else if (scenario == "scenario3") {
            SimpleStatement.newInstance(s"SELECT COUNT(*) as count FROM missing_records_test_$scenario WHERE phase = ? ALLOW FILTERING", filterValue)
          } else {
            SimpleStatement.newInstance(s"SELECT COUNT(*) as count FROM missing_records_test_$scenario")
          }
        case None =>
          SimpleStatement.newInstance(s"SELECT COUNT(*) as count FROM missing_records_test_$scenario")
      }
      
      val result = session.execute(query)
      result.one().getLong("count")
    } catch {
      case ex: Exception =>
        println(s"     ⚠️ Count query failed for $scenario: ${ex.getMessage}")
        0L
    }
  }
  
  def showRecordBreakdownBySession(session: CqlSession, scenario: String): Unit = {
    try {
      val query = SimpleStatement.newInstance(s"SELECT session_type, COUNT(*) as count FROM missing_records_test_$scenario GROUP BY session_type")
      val results = session.execute(query)
      
      println("     📋 Detailed breakdown:")
      results.forEach { row =>
        val sessionType = row.getString("session_type")
        val count = row.getLong("count")
        println(s"       • $sessionType: $count records")
      }
    } catch {
      case ex: Exception =>
        println(s"     ⚠️ Breakdown query failed: ${ex.getMessage}")
    }
  }
  
  def showSessionKeyspaces(session: CqlSession, sessionName: String): Unit = {
    try {
      val query = SimpleStatement.newInstance("SELECT keyspace_name FROM system.local")
      val result = session.execute(query)
      val keyspace = Option(result.one()).map(_.getString("keyspace_name")).getOrElse("unknown")
      println(s"     📍 $sessionName session keyspace: $keyspace")
    } catch {
      case ex: Exception =>
        println(s"     ⚠️ Could not determine keyspace for $sessionName: ${ex.getMessage}")
    }
  }
}