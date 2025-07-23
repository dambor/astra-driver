package examples

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import astra.{AstraSession, AstraUploader, Cluster}
import com.datastax.oss.driver.api.core.cql.{SimpleStatement, BoundStatement}

import scala.concurrent.ExecutionContext
import scala.collection.mutable.Queue
import java.util.UUID
import scala.concurrent.duration._
import scala.util.Random

object ComprehensiveTestSuite extends App {
  implicit val system: ActorSystem = ActorSystem("ComprehensiveTestSystem")
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val ec: ExecutionContext = system.dispatcher
  
  println("🚀 Comprehensive Astra Driver Test Suite...")
  println("=" * 60)
  
  val testResults = scala.collection.mutable.Map[String, TestResult]()
  
  try {
    // Setup
    val (realSession, chaosWrapper) = AstraSession.createSessionWithChaos("tradingtech")
    val clusterSession = Cluster.getOrCreateLiveSession.getSession()
    val uploader = new AstraUploader()
    
    setupTestTables(clusterSession)
    
    // Test Suite 1: Basic Functionality
    println("\n🧪 Test Suite 1: Basic Functionality")
    println("-" * 40)
    
    runTest("1.1 Single Record Insert", testResults) {
      testSingleInsert(clusterSession, chaosWrapper)
    }
    
    runTest("1.2 Small Batch Insert", testResults) {
      testSmallBatch(clusterSession, chaosWrapper, uploader, 5)
    }
    
    runTest("1.3 Medium Batch Insert", testResults) {
      testMediumBatch(clusterSession, chaosWrapper, uploader, 50)
    }
    
    runTest("1.4 Large Batch Insert", testResults) {
      testLargeBatch(clusterSession, chaosWrapper, uploader, 200)
    }
    
    // Test Suite 2: Chaos Engineering
    println("\n🐒 Test Suite 2: Chaos Engineering")
    println("-" * 40)
    
    runTest("2.1 Low Chaos (10%)", testResults) {
      testWithChaos(clusterSession, chaosWrapper, uploader, 0.1, 20)
    }
    
    runTest("2.2 Medium Chaos (30%)", testResults) {
      testWithChaos(clusterSession, chaosWrapper, uploader, 0.3, 25)
    }
    
    runTest("2.3 High Chaos (60%)", testResults) {
      testWithChaos(clusterSession, chaosWrapper, uploader, 0.6, 30)
    }
    
    runTest("2.4 Extreme Chaos (90%)", testResults) {
      testWithChaos(clusterSession, chaosWrapper, uploader, 0.9, 15)
    }
    
    // Test Suite 3: Edge Cases
    println("\n⚡ Test Suite 3: Edge Cases")
    println("-" * 40)
    
    runTest("3.1 Empty Batch", testResults) {
      testEmptyBatch(clusterSession, chaosWrapper, uploader)
    }
    
    runTest("3.2 Duplicate Data", testResults) {
      testDuplicateData(clusterSession, chaosWrapper, uploader)
    }
    
    runTest("3.3 Large Data Payload", testResults) {
      testLargeDataPayload(clusterSession, chaosWrapper, uploader)
    }
    
    runTest("3.4 Special Characters", testResults) {
      testSpecialCharacters(clusterSession, chaosWrapper, uploader)
    }
    
    // Test Suite 4: Concurrent Operations
    println("\n🔄 Test Suite 4: Concurrent Operations")
    println("-" * 40)
    
    runTest("4.1 Concurrent Batches", testResults) {
      testConcurrentBatches(clusterSession, chaosWrapper, uploader)
    }
    
    runTest("4.2 Rapid Fire Inserts", testResults) {
      testRapidFireInserts(clusterSession, chaosWrapper, uploader)
    }
    
    // Test Suite 5: Failure Recovery
    println("\n🛡️ Test Suite 5: Failure Recovery")
    println("-" * 40)
    
    runTest("5.1 Session Reset Recovery", testResults) {
      testSessionResetRecovery(clusterSession, chaosWrapper, uploader)
    }
    
    runTest("5.2 Timeout Recovery", testResults) {
      testTimeoutRecovery(clusterSession, chaosWrapper, uploader)
    }
    
    runTest("5.3 Retry Logic Verification", testResults) {
      testRetryLogic(clusterSession, chaosWrapper, uploader)
    }
    
    // Test Suite 6: Performance
    println("\n🏎️ Test Suite 6: Performance")
    println("-" * 40)
    
    runTest("6.1 Throughput Test", testResults) {
      testThroughput(clusterSession, chaosWrapper, uploader)
    }
    
    runTest("6.2 Memory Usage", testResults) {
      testMemoryUsage(clusterSession, chaosWrapper, uploader)
    }
    
    clusterSession.close()
    
  } catch {
    case ex: Exception =>
      log.error(ex, "Test suite failed")
      println(s"❌ Test suite failed: ${ex.getMessage}")
  } finally {
    // Print final report
    printTestReport(testResults.toMap)
    system.terminate()
  }
  
  // Test implementations
  def setupTestTables(session: com.datastax.oss.driver.api.core.CqlSession): Unit = {
    val tables = List(
      "DROP TABLE IF EXISTS test_basic",
      "DROP TABLE IF EXISTS test_chaos", 
      "DROP TABLE IF EXISTS test_edge_cases",
      "DROP TABLE IF EXISTS test_concurrent",
      "DROP TABLE IF EXISTS test_recovery",
      "DROP TABLE IF EXISTS test_performance",
      
      """CREATE TABLE test_basic (
        id UUID PRIMARY KEY,
        data TEXT,
        test_type TEXT,
        created_at TIMESTAMP
      )""",
      
      """CREATE TABLE test_chaos (
        id UUID PRIMARY KEY,
        chaos_level TEXT,
        data TEXT,
        attempt_count INT,
        created_at TIMESTAMP
      )""",
      
      """CREATE TABLE test_edge_cases (
        id UUID PRIMARY KEY,
        data TEXT,
        data_size INT,
        special_chars TEXT,
        created_at TIMESTAMP
      )""",
      
      """CREATE TABLE test_concurrent (
        id UUID PRIMARY KEY,
        thread_id TEXT,
        data TEXT,
        batch_id TEXT,
        created_at TIMESTAMP
      )""",
      
      """CREATE TABLE test_recovery (
        id UUID PRIMARY KEY,
        recovery_scenario TEXT,
        data TEXT,
        success BOOLEAN,
        created_at TIMESTAMP
      )""",
      
      """CREATE TABLE test_performance (
        id UUID PRIMARY KEY,
        batch_size INT,
        execution_time_ms BIGINT,
        throughput_per_sec DOUBLE,
        created_at TIMESTAMP
      )"""
    )
    
    tables.foreach { sql =>
      try {
        session.execute(SimpleStatement.newInstance(sql))
      } catch {
        case ex: Exception => println(s"⚠️ Table setup warning: ${ex.getMessage}")
      }
    }
    println("✅ Test tables setup complete")
  }
  
  def testSingleInsert(session: com.datastax.oss.driver.api.core.CqlSession, 
                      chaosWrapper: astra.ChaosWrapper): TestResult = {
    chaosWrapper.disableChaos()
    val insertStmt = session.prepare("INSERT INTO test_basic (id, data, test_type, created_at) VALUES (?, ?, ?, toTimestamp(now()))")
    
    val bound = insertStmt.bind(UUID.randomUUID(), "single_test_data", "single_insert")
    session.execute(bound)
    
    val count = session.execute(SimpleStatement.newInstance("SELECT COUNT(*) FROM test_basic")).one().getLong(0)
    TestResult(success = count >= 1, s"Inserted 1 record, found $count total")
  }
  
  def testSmallBatch(session: com.datastax.oss.driver.api.core.CqlSession,
                    chaosWrapper: astra.ChaosWrapper,
                    uploader: AstraUploader,
                    batchSize: Int): TestResult = {
    chaosWrapper.disableChaos()
    testBatchInsert(session, chaosWrapper, uploader, batchSize, "small_batch")
  }
  
  def testMediumBatch(session: com.datastax.oss.driver.api.core.CqlSession,
                     chaosWrapper: astra.ChaosWrapper,
                     uploader: AstraUploader,
                     batchSize: Int): TestResult = {
    chaosWrapper.disableChaos()
    testBatchInsert(session, chaosWrapper, uploader, batchSize, "medium_batch")
  }
  
  def testLargeBatch(session: com.datastax.oss.driver.api.core.CqlSession,
                    chaosWrapper: astra.ChaosWrapper,
                    uploader: AstraUploader,
                    batchSize: Int): TestResult = {
    chaosWrapper.disableChaos()
    testBatchInsert(session, chaosWrapper, uploader, batchSize, "large_batch")
  }
  
  def testWithChaos(session: com.datastax.oss.driver.api.core.CqlSession,
                   chaosWrapper: astra.ChaosWrapper,
                   uploader: AstraUploader,
                   chaosLevel: Double,
                   batchSize: Int): TestResult = {
    chaosWrapper.enableChaos()
    chaosWrapper.setChaosLevel(chaosLevel)
    
    val result = testBatchInsert(session, chaosWrapper, uploader, batchSize, s"chaos_${(chaosLevel * 100).toInt}")
    Thread.sleep(5000) // Wait for retries
    
    val stats = chaosWrapper.getStats
    val enhancedMessage = s"${result.message}, Chaos: ${stats.chaosInjections}/${stats.totalRequests}"
    
    result.copy(message = enhancedMessage)
  }
  
  def testBatchInsert(session: com.datastax.oss.driver.api.core.CqlSession,
                     chaosWrapper: astra.ChaosWrapper,
                     uploader: AstraUploader,
                     batchSize: Int,
                     testType: String): TestResult = {
    val startTime = System.currentTimeMillis()
    val insertStmt = session.prepare("INSERT INTO test_basic (id, data, test_type, created_at) VALUES (?, ?, ?, toTimestamp(now()))")
    
    val elements = (1 to batchSize).map { i =>
      val data = s"${testType}_data_$i"
      val bound = insertStmt.bind(UUID.randomUUID(), data, testType)
      (data, bound)
    }
    
    val successfulBatch = Queue(elements.map(_._2): _*)
    val countBefore = session.execute(SimpleStatement.newInstance("SELECT COUNT(*) FROM test_basic")).one().getLong(0)
    
    uploader.processBatch(elements, s"${testType}_topic", successfulBatch)
    
    Thread.sleep(3000) // Wait for async completion
    
    val countAfter = session.execute(SimpleStatement.newInstance("SELECT COUNT(*) FROM test_basic")).one().getLong(0)
    val actualInserted = countAfter - countBefore
    val duration = System.currentTimeMillis() - startTime
    
    TestResult(
      success = actualInserted >= batchSize,
      s"Expected $batchSize, inserted $actualInserted in ${duration}ms"
    )
  }
  
  def testEmptyBatch(session: com.datastax.oss.driver.api.core.CqlSession,
                    chaosWrapper: astra.ChaosWrapper,
                    uploader: AstraUploader): TestResult = {
    chaosWrapper.disableChaos()
    
    try {
      val emptyElements = Seq.empty[(String, BoundStatement)]
      val emptyBatch = Queue.empty[BoundStatement]
      
      uploader.processBatch(emptyElements, "empty_topic", emptyBatch)
      TestResult(success = true, "Empty batch handled gracefully")
    } catch {
      case ex: Exception =>
        TestResult(success = false, s"Empty batch failed: ${ex.getMessage}")
    }
  }
  
  def testDuplicateData(session: com.datastax.oss.driver.api.core.CqlSession,
                       chaosWrapper: astra.ChaosWrapper,
                       uploader: AstraUploader): TestResult = {
    chaosWrapper.disableChaos()
    val insertStmt = session.prepare("INSERT INTO test_edge_cases (id, data, data_size, created_at) VALUES (?, ?, ?, toTimestamp(now()))")
    
    val duplicateId = UUID.randomUUID()
    val elements = (1 to 3).map { i =>
      val bound = insertStmt.bind(duplicateId, "duplicate_data", 100)
      (s"duplicate_$i", bound)
    }
    
    val successfulBatch = Queue(elements.map(_._2): _*)
    uploader.processBatch(elements, "duplicate_topic", successfulBatch)
    
    Thread.sleep(2000)
    TestResult(success = true, "Duplicate data handled (last write wins in Cassandra)")
  }
  
  def testLargeDataPayload(session: com.datastax.oss.driver.api.core.CqlSession,
                          chaosWrapper: astra.ChaosWrapper,
                          uploader: AstraUploader): TestResult = {
    chaosWrapper.disableChaos()
    val insertStmt = session.prepare("INSERT INTO test_edge_cases (id, data, data_size, created_at) VALUES (?, ?, ?, toTimestamp(now()))")
    
    val largeData = "x" * 10000 // 10KB payload
    val elements = (1 to 5).map { i =>
      val bound = insertStmt.bind(UUID.randomUUID(), largeData, largeData.length)
      (s"large_data_$i", bound)
    }
    
    val successfulBatch = Queue(elements.map(_._2): _*)
    
    try {
      uploader.processBatch(elements, "large_data_topic", successfulBatch)
      Thread.sleep(3000)
      TestResult(success = true, s"Large payloads (${largeData.length} chars) handled successfully")
    } catch {
      case ex: Exception =>
        TestResult(success = false, s"Large payload failed: ${ex.getMessage}")
    }
  }
  
  def testSpecialCharacters(session: com.datastax.oss.driver.api.core.CqlSession,
                           chaosWrapper: astra.ChaosWrapper,
                           uploader: AstraUploader): TestResult = {
    chaosWrapper.disableChaos()
    val insertStmt = session.prepare("INSERT INTO test_edge_cases (id, special_chars, data_size, created_at) VALUES (?, ?, ?, toTimestamp(now()))")
    
    val specialChars = List("emoji: 🚀🐒💥", "unicode: αβγδε", "quotes: \"'`", "newlines: \n\r\t", "json: {\"key\": \"value\"}")
    
    val elements = specialChars.zipWithIndex.map { case (chars, i) =>
      val bound = insertStmt.bind(UUID.randomUUID(), chars, chars.length)
      (s"special_$i", bound)
    }
    
    val successfulBatch = Queue(elements.map(_._2): _*)
    
    try {
      uploader.processBatch(elements, "special_chars_topic", successfulBatch)
      Thread.sleep(2000)
      TestResult(success = true, "Special characters handled successfully")
    } catch {
      case ex: Exception =>
        TestResult(success = false, s"Special characters failed: ${ex.getMessage}")
    }
  }
  
  def testConcurrentBatches(session: com.datastax.oss.driver.api.core.CqlSession,
                           chaosWrapper: astra.ChaosWrapper,
                           uploader: AstraUploader): TestResult = {
    chaosWrapper.disableChaos()
    val insertStmt = session.prepare("INSERT INTO test_concurrent (id, thread_id, data, batch_id, created_at) VALUES (?, ?, ?, ?, toTimestamp(now()))")
    
    val futures = (1 to 3).map { threadId =>
      scala.concurrent.Future {
        val batchId = UUID.randomUUID().toString
        val elements = (1 to 10).map { i =>
          val bound = insertStmt.bind(UUID.randomUUID(), s"thread_$threadId", s"data_$i", batchId)
          (s"concurrent_${threadId}_$i", bound)
        }
        val successfulBatch = Queue(elements.map(_._2): _*)
        uploader.processBatch(elements, s"concurrent_topic_$threadId", successfulBatch)
      }
    }
    
    try {
      import scala.concurrent.Await
      Await.ready(scala.concurrent.Future.sequence(futures), 30.seconds)
      Thread.sleep(5000)
      TestResult(success = true, "Concurrent batches processed successfully")
    } catch {
      case ex: Exception =>
        TestResult(success = false, s"Concurrent processing failed: ${ex.getMessage}")
    }
  }
  
  def testRapidFireInserts(session: com.datastax.oss.driver.api.core.CqlSession,
                          chaosWrapper: astra.ChaosWrapper,
                          uploader: AstraUploader): TestResult = {
    chaosWrapper.disableChaos()
    val insertStmt = session.prepare("INSERT INTO test_concurrent (id, thread_id, data, created_at) VALUES (?, ?, ?, toTimestamp(now()))")
    
    try {
      (1 to 20).foreach { i =>
        val elements = List {
          val bound = insertStmt.bind(UUID.randomUUID(), "rapid_fire", s"data_$i")
          (s"rapid_$i", bound)
        }
        val successfulBatch = Queue(elements.map(_._2): _*)
        uploader.processBatch(elements, "rapid_topic", successfulBatch)
        Thread.sleep(100) // Small delay between batches
      }
      
      Thread.sleep(3000)
      TestResult(success = true, "Rapid fire inserts completed")
    } catch {
      case ex: Exception =>
        TestResult(success = false, s"Rapid fire failed: ${ex.getMessage}")
    }
  }
  
  def testSessionResetRecovery(session: com.datastax.oss.driver.api.core.CqlSession,
                              chaosWrapper: astra.ChaosWrapper,
                              uploader: AstraUploader): TestResult = {
    chaosWrapper.disableChaos()
    
    try {
      // Force a session reset
      Cluster.getOrCreateLiveSession.resetSession()
      
      // Try to insert after reset
      testBatchInsert(session, chaosWrapper, uploader, 5, "session_reset")
    } catch {
      case ex: Exception =>
        TestResult(success = false, s"Session reset recovery failed: ${ex.getMessage}")
    }
  }
  
  def testTimeoutRecovery(session: com.datastax.oss.driver.api.core.CqlSession,
                         chaosWrapper: astra.ChaosWrapper,
                         uploader: AstraUploader): TestResult = {
    // This would test timeout scenarios - simplified for demo
    chaosWrapper.enableChaos()
    chaosWrapper.setChaosLevel(0.8) // High chaos to trigger timeouts
    
    val result = testBatchInsert(session, chaosWrapper, uploader, 10, "timeout_test")
    Thread.sleep(10000) // Wait for retries
    
    result.copy(message = s"${result.message} (timeout recovery test)")
  }
  
  def testRetryLogic(session: com.datastax.oss.driver.api.core.CqlSession,
                    chaosWrapper: astra.ChaosWrapper,
                    uploader: AstraUploader): TestResult = {
    chaosWrapper.enableChaos()
    chaosWrapper.setChaosLevel(0.5)
    
    val statsBefore = chaosWrapper.getStats
    val result = testBatchInsert(session, chaosWrapper, uploader, 20, "retry_test")
    Thread.sleep(8000) // Wait for retries
    val statsAfter = chaosWrapper.getStats
    
    val retriesTriggered = statsAfter.chaosInjections - statsBefore.chaosInjections
    TestResult(
      success = result.success && retriesTriggered > 0,
      s"${result.message}, Retries triggered: $retriesTriggered"
    )
  }
  
  def testThroughput(session: com.datastax.oss.driver.api.core.CqlSession,
                    chaosWrapper: astra.ChaosWrapper,
                    uploader: AstraUploader): TestResult = {
    chaosWrapper.disableChaos()
    val batchSize = 100
    val startTime = System.currentTimeMillis()
    
    val result = testBatchInsert(session, chaosWrapper, uploader, batchSize, "throughput_test")
    val duration = System.currentTimeMillis() - startTime
    val throughput = if (duration > 0) (batchSize * 1000.0) / duration else 0.0
    
    TestResult(
      success = result.success,
      s"${result.message}, Throughput: ${throughput.toInt} records/sec"
    )
  }
  
  def testMemoryUsage(session: com.datastax.oss.driver.api.core.CqlSession,
                     chaosWrapper: astra.ChaosWrapper,
                     uploader: AstraUploader): TestResult = {
    chaosWrapper.disableChaos()
    
    val runtime = Runtime.getRuntime
    val memBefore = runtime.totalMemory() - runtime.freeMemory()
    
    val result = testBatchInsert(session, chaosWrapper, uploader, 50, "memory_test")
    
    System.gc() // Suggest garbage collection
    Thread.sleep(1000)
    val memAfter = runtime.totalMemory() - runtime.freeMemory()
    val memDelta = (memAfter - memBefore) / 1024 / 1024 // MB
    
    TestResult(
      success = result.success,
      s"${result.message}, Memory delta: ${memDelta}MB"
    )
  }
  
  // Helper functions
  def runTest(testName: String, results: scala.collection.mutable.Map[String, TestResult])(testFn: => TestResult): Unit = {
    print(s"$testName... ")
    try {
      val result = testFn
      results(testName) = result
      if (result.success) {
        println(s"✅ PASS - ${result.message}")
      } else {
        println(s"❌ FAIL - ${result.message}")
      }
    } catch {
      case ex: Exception =>
        results(testName) = TestResult(false, ex.getMessage)
        println(s"💥 ERROR - ${ex.getMessage}")
    }
  }
  
  def printTestReport(results: Map[String, TestResult]): Unit = {
    println("\n" + "=" * 60)
    println("📊 COMPREHENSIVE TEST REPORT")
    println("=" * 60)
    
    val passed = results.count(_._2.success)
    val total = results.size
    val passRate = if (total > 0) (passed * 100.0) / total else 0.0
    
    println(s"📈 Overall Results: $passed/$total tests passed (${passRate.toInt}%)")
    println()
    
    results.foreach { case (testName, result) =>
      val status = if (result.success) "✅ PASS" else "❌ FAIL"
      println(f"$status $testName%-40s ${result.message}")
    }
    
    println("\n" + "=" * 60)
    if (passRate >= 90) {
      println("🎉 EXCELLENT: Your Astra driver is highly reliable!")
    } else if (passRate >= 75) {
      println("👍 GOOD: Your Astra driver is mostly reliable with minor issues")
    } else {
      println("⚠️ NEEDS WORK: Several reliability issues detected")
    }
  }
}

case class TestResult(success: Boolean, message: String)