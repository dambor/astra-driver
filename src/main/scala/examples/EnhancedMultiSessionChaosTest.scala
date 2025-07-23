package examples

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.event.{Logging, LoggingAdapter}
import astra.{AstraSession, AstraUploader, Cluster}
import com.datastax.oss.driver.api.core.cql.{SimpleStatement, BoundStatement, PreparedStatement}
import com.datastax.oss.driver.api.core.CqlSession

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.collection.mutable.Queue
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit, ConcurrentHashMap}
import scala.util.{Random, Try, Success, Failure}

object EnhancedMultiSessionChaosTest extends App {
  implicit val system: ActorSystem = ActorSystem("EnhancedMultiSessionChaosTestSystem")
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val ec: ExecutionContext = system.dispatcher
  
  println("🏭 Enhanced Multi-Session Multi-Batch Chaos Test - Production Simulation")
  println("=" * 80)
  
  try {
    // Create test table with additional tracking columns
    val setupSession = Cluster.getOrCreateLiveSession.getSession()
    val createTable = SimpleStatement.newInstance(
      """
      CREATE TABLE IF NOT EXISTS enhanced_multi_session_test (
        id UUID PRIMARY KEY,
        batch_id TEXT,
        message_data TEXT,
        topic TEXT,
        session_id TEXT,
        worker_id TEXT,
        test_phase TEXT,
        attempt_count INT,
        processing_start_time TIMESTAMP,
        processing_end_time TIMESTAMP,
        created_at TIMESTAMP,
        uploader_session_id TEXT,
        error_details TEXT
      )
      """
    )
    setupSession.execute(createTable)
    println("✅ Enhanced table created!")
    
    // CLEAN START
    println("🧹 Clearing table...")
    val truncateTable = SimpleStatement.newInstance("TRUNCATE enhanced_multi_session_test")
    setupSession.execute(truncateTable)
    
    // Verify clean start
    val initialCount = setupSession.execute(SimpleStatement.newInstance("SELECT COUNT(*) as count FROM enhanced_multi_session_test")).one().getLong("count")
    println(s"📊 Initial count: $initialCount (should be 0)")
    
    val testRunId = UUID.randomUUID().toString.take(8)
    println(s"🚀 Starting enhanced multi-session test (Test Run: $testRunId)")
    
    // Enhanced Test Configuration
    val testConfig = EnhancedTestConfig(
      numSessions = 3,           // 3 different database sessions
      numWorkersPerSession = 2,  // 2 workers per session
      batchesPerWorker = 5,      // 5 batches per worker
      recordsPerBatch = 100,     // 100 records per batch
      chaosLevel = 0.3,          // 30% chaos
      retryAttempts = 3,         // Retry failed batches up to 3 times
      sessionValidationEnabled = true,  // Validate session consistency
      detailedLogging = true     // Enable detailed failure logging
    )
    
    println(s"📋 Enhanced Test Configuration:")
    println(s"   • Sessions: ${testConfig.numSessions}")
    println(s"   • Workers per session: ${testConfig.numWorkersPerSession}")
    println(s"   • Batches per worker: ${testConfig.batchesPerWorker}")
    println(s"   • Records per batch: ${testConfig.recordsPerBatch}")
    println(s"   • Total expected records: ${testConfig.totalExpectedRecords}")
    println(s"   • Chaos level: ${(testConfig.chaosLevel * 100).toInt}%")
    println(s"   • Retry attempts: ${testConfig.retryAttempts}")
    println(s"   • Session validation: ${testConfig.sessionValidationEnabled}")
    
    // Create multiple sessions with enhanced tracking
    val sessions = createEnhancedSessions(testConfig.numSessions)
    println(s"✅ Created ${sessions.size} database sessions")
    
    // Create enhanced workers for each session
    val workers = createEnhancedWorkers(sessions, testConfig)
    println(s"✅ Created ${workers.size} enhanced worker actors")
    
    // Create enhanced coordinator with failure tracking
    val coordinator = system.actorOf(Props(new EnhancedTestCoordinator(workers, testConfig, testRunId)), "enhanced-coordinator")
    
    println("\n🎬 Starting enhanced multi-session chaos test...")
    coordinator ! StartTest
    
    // Wait for all workers to complete with progress monitoring
    val maxWaitTime = 180000 // 3 minutes
    val startTime = System.currentTimeMillis()
    
    while (System.currentTimeMillis() - startTime < maxWaitTime) {
      Thread.sleep(5000) // Check every 5 seconds
      // Could add progress polling here
    }
    
    // Enhanced verification and analysis
    println("\n" + "=" * 80)
    println("🔍 ENHANCED FINAL VERIFICATION")
    println("=" * 80)
    
    val finalCount = setupSession.execute(SimpleStatement.newInstance("SELECT COUNT(*) as count FROM enhanced_multi_session_test")).one().getLong("count")
    val expectedTotal = testConfig.totalExpectedRecords
    
    println(s"📊 FINAL COUNT: $finalCount")
    println(s"📊 EXPECTED: $expectedTotal")
    
    val successRate = (finalCount.toDouble / expectedTotal * 100)
    
    if (finalCount == expectedTotal) {
      println(s"🎉 SUCCESS: All $expectedTotal records inserted correctly!")
      println(s"✅ Multi-session environment handles all scenarios correctly!")
    } else if (finalCount < expectedTotal) {
      println(s"🚨 MISSING RECORDS: Expected $expectedTotal, found $finalCount")
      println(s"🚨 Missing ${expectedTotal - finalCount} records")
      val lossPercentage = ((expectedTotal - finalCount).toDouble / expectedTotal * 100)
      println(f"🚨 Data loss: $lossPercentage%.2f%%")
      println(f"📊 Success rate: $successRate%.2f%%")
      println(s"🚨 THIS REPRODUCES THE PRODUCTION BUG!")
    } else {
      println(s"🔄 EXTRA RECORDS: Expected $expectedTotal, found $finalCount") 
      println(s"🔄 Extra ${finalCount - expectedTotal} records - likely retry duplicates")
      println(f"📊 Success rate: $successRate%.2f%% (over 100% due to retries)")
    }
    
    // Enhanced detailed analysis
    analyzeEnhancedResults(setupSession, testConfig)
    
    // Cleanup
    sessions.foreach(_.session.close())
    
  } catch {
    case ex: Exception =>
      log.error(ex, "Enhanced multi-session chaos test failed")
      println(s"❌ Test failed: ${ex.getMessage}")
      ex.printStackTrace()
  } finally {
    println("🔚 Terminating actor system...")
    system.terminate()
  }
  
  def createEnhancedSessions(numSessions: Int): List[EnhancedSessionInfo] = {
    (1 to numSessions).map { i =>
      val sessionType = i match {
        case 1 => 
          val session = Cluster.getOrCreateLiveSession.getSession()
          val sessionId = s"cluster_session_$i"
          EnhancedSessionInfo(sessionId, session, "cluster", getSessionMetadata(session))
        case 2 => 
          val session = AstraSession.createSession("tradingtech") 
          val sessionId = s"direct_session_$i"
          EnhancedSessionInfo(sessionId, session, "direct", getSessionMetadata(session))
        case 3 =>
          val (session, chaosWrapper) = AstraSession.createSessionWithChaos("tradingtech")
          chaosWrapper.enableChaos()
          chaosWrapper.setChaosLevel(0.3)
          val sessionId = s"chaos_session_$i"
          EnhancedSessionInfo(sessionId, session, "chaos", getSessionMetadata(session), Some(chaosWrapper))
        case _ =>
          val session = if (Random.nextBoolean()) {
            Cluster.getOrCreateLiveSession.getSession()
          } else {
            AstraSession.createSession("tradingtech")
          }
          val sessionId = s"mixed_session_$i"
          EnhancedSessionInfo(sessionId, session, "mixed", getSessionMetadata(session))
      }
      
      println(s"   📡 Created ${sessionType.sessionType} session: ${sessionType.sessionId}")
      println(s"      └─ Metadata: ${sessionType.metadata}")
      sessionType
    }.toList
  }
  
  def getSessionMetadata(session: CqlSession): String = {
    try {
      val metadata = session.getMetadata
      s"cluster=${metadata.getClusterName.orElse("unknown")}, keyspace=${session.getKeyspace.orElse("none")}"
    } catch {
      case _: Exception => "metadata_unavailable"
    }
  }
  
  def createEnhancedWorkers(sessions: List[EnhancedSessionInfo], config: EnhancedTestConfig): List[ActorRef] = {
    val workers = scala.collection.mutable.ListBuffer[ActorRef]()
    
    sessions.foreach { sessionInfo =>
      (1 to config.numWorkersPerSession).foreach { workerNum =>
        val workerId = s"${sessionInfo.sessionId}_worker_$workerNum"
        val worker = system.actorOf(Props(new EnhancedBatchWorker(sessionInfo, config, workerId)), workerId)
        workers += worker
        println(s"   👷 Created enhanced worker: $workerId")
      }
    }
    
    workers.toList
  }
  
  def analyzeEnhancedResults(session: CqlSession, config: EnhancedTestConfig): Unit = {
    try {
      println("\n📊 Enhanced Detailed Analysis:")
      
      // Session consistency analysis
      if (config.sessionValidationEnabled) {
        println("🔍 Session Consistency Analysis:")
        val sessionConsistencyQuery = SimpleStatement.newInstance(
          """
          SELECT session_id, uploader_session_id, COUNT(*) as count 
          FROM enhanced_multi_session_test 
          GROUP BY session_id, uploader_session_id 
          ALLOW FILTERING
          """
        )
        
        val sessionResults = session.execute(sessionConsistencyQuery)
        sessionResults.forEach { row =>
          val sessionId = row.getString("session_id")
          val uploaderSessionId = row.getString("uploader_session_id")
          val count = row.getLong("count")
          val consistent = sessionId == uploaderSessionId
          val status = if (consistent) "✅" else "🚨"
          println(s"   $status Worker Session: $sessionId, Uploader Session: $uploaderSessionId, Records: $count")
        }
      }
      
      // Retry analysis
      println("\n🔄 Retry Analysis:")
      val retryQuery = SimpleStatement.newInstance(
        "SELECT attempt_count, COUNT(*) as count FROM enhanced_multi_session_test GROUP BY attempt_count ALLOW FILTERING"
      )
      val retryResults = session.execute(retryQuery)
      retryResults.forEach { row =>
        val attemptCount = row.getInt("attempt_count")
        val count = row.getLong("count")
        val status = if (attemptCount == 1) "✅" else "🔄"
        println(s"   $status Attempt $attemptCount: $count records")
      }
      
      // Error analysis
      println("\n❌ Error Analysis:")
      val errorQuery = SimpleStatement.newInstance(
        "SELECT error_details, COUNT(*) as count FROM enhanced_multi_session_test WHERE error_details IS NOT NULL GROUP BY error_details ALLOW FILTERING"
      )
      val errorResults = session.execute(errorQuery)
      var hasErrors = false
      errorResults.forEach { row =>
        hasErrors = true
        val errorDetails = row.getString("error_details")
        val count = row.getLong("count")
        println(s"   🚨 Error: $errorDetails, Count: $count")
      }
      if (!hasErrors) {
        println("   ✅ No error details recorded")
      }
      
      // Processing time analysis
      println("\n⏱️ Processing Time Analysis:")
      val timingQuery = SimpleStatement.newInstance(
        """
        SELECT 
          AVG(processing_end_time - processing_start_time) as avg_processing_time,
          MIN(processing_end_time - processing_start_time) as min_processing_time,
          MAX(processing_end_time - processing_start_time) as max_processing_time
        FROM enhanced_multi_session_test 
        WHERE processing_start_time IS NOT NULL AND processing_end_time IS NOT NULL
        """
      )
      
      try {
        val timingResult = session.execute(timingQuery).one()
        if (timingResult != null) {
          println(s"   📈 Average processing time: ${timingResult.getLong("avg_processing_time")}ms")
          println(s"   📉 Min processing time: ${timingResult.getLong("min_processing_time")}ms")
          println(s"   📊 Max processing time: ${timingResult.getLong("max_processing_time")}ms")
        }
      } catch {
        case ex: Exception =>
          println(s"   ⚠️ Could not analyze processing times: ${ex.getMessage}")
      }
      
      // Original analyses (enhanced)
      analyzeBySession(session, config)
      analyzeByWorker(session, config)
      analyzeBatchPatterns(session, config)
      
    } catch {
      case ex: Exception =>
        println(s"⚠️ Could not perform enhanced analysis: ${ex.getMessage}")
        ex.printStackTrace()
    }
  }
  
  def analyzeBySession(session: CqlSession, config: EnhancedTestConfig): Unit = {
    println("\n📡 Records by session:")
    val sessionQuery = SimpleStatement.newInstance("SELECT session_id, COUNT(*) as count FROM enhanced_multi_session_test GROUP BY session_id ALLOW FILTERING")
    val sessionResults = session.execute(sessionQuery)
    
    sessionResults.forEach { row =>
      val sessionId = row.getString("session_id")
      val count = row.getLong("count")
      val expectedPerSession = config.numWorkersPerSession * config.batchesPerWorker * config.recordsPerBatch
      val successRate = (count.toDouble / expectedPerSession * 100)
      val status = if (count >= expectedPerSession * 0.95) "✅" else "🚨" // Allow 5% tolerance
      println(f"   $status $sessionId: $count/$expectedPerSession records ($successRate%.1f%%)")
    }
  }
  
  def analyzeByWorker(session: CqlSession, config: EnhancedTestConfig): Unit = {
    println("\n👷 Records by worker:")
    val workerQuery = SimpleStatement.newInstance("SELECT worker_id, COUNT(*) as count FROM enhanced_multi_session_test GROUP BY worker_id ALLOW FILTERING")
    val workerResults = session.execute(workerQuery)
    
    workerResults.forEach { row =>
      val workerId = row.getString("worker_id")
      val count = row.getLong("count")
      val expectedPerWorker = config.batchesPerWorker * config.recordsPerBatch
      val successRate = (count.toDouble / expectedPerWorker * 100)
      val status = if (count >= expectedPerWorker * 0.95) "✅" else "🚨" // Allow 5% tolerance
      println(f"   $status $workerId: $count/$expectedPerWorker records ($successRate%.1f%%)")
    }
  }
  
  def analyzeBatchPatterns(session: CqlSession, config: EnhancedTestConfig): Unit = {
    println("\n📦 Enhanced Batch Analysis:")
    val distinctBatches = session.execute(SimpleStatement.newInstance("SELECT COUNT(DISTINCT batch_id) as batch_count FROM enhanced_multi_session_test")).one().getLong("batch_count")
    val expectedBatches = config.numSessions * config.numWorkersPerSession * config.batchesPerWorker
    
    println(s"   • Expected batches: $expectedBatches")
    println(s"   • Found batches: $distinctBatches")
    
    if (distinctBatches < expectedBatches) {
      println(s"   🚨 Missing ${expectedBatches - distinctBatches} entire batches!")
      
      // Find which batches are missing records
      val batchSizeQuery = SimpleStatement.newInstance(
        "SELECT batch_id, COUNT(*) as record_count FROM enhanced_multi_session_test GROUP BY batch_id ALLOW FILTERING"
      )
      val batchResults = session.execute(batchSizeQuery)
      
      println("   📊 Batch completeness:")
      batchResults.forEach { row =>
        val batchId = row.getString("batch_id")
        val recordCount = row.getLong("record_count")
        val expectedRecords = config.recordsPerBatch
        val status = if (recordCount == expectedRecords) "✅" else "🚨"
        if (recordCount != expectedRecords) {
          println(f"   $status Batch $batchId: $recordCount/$expectedRecords records")
        }
      }
    }
  }
}

// Enhanced data classes
case class EnhancedTestConfig(
  numSessions: Int,
  numWorkersPerSession: Int,
  batchesPerWorker: Int,
  recordsPerBatch: Int,
  chaosLevel: Double,
  retryAttempts: Int,
  sessionValidationEnabled: Boolean,
  detailedLogging: Boolean
) {
  def totalExpectedRecords: Int = numSessions * numWorkersPerSession * batchesPerWorker * recordsPerBatch
}

case class EnhancedSessionInfo(
  sessionId: String,
  session: CqlSession,
  sessionType: String,
  metadata: String,
  chaosWrapper: Option[astra.ChaosWrapper] = None
)

// Enhanced Actor messages
case class ProcessBatchWithRetry(batchNum: Int, attemptCount: Int = 1) extends TestMessage
case class BatchCompletedWithDetails(
  workerId: String, 
  batchNum: Int, 
  recordCount: Int, 
  success: Boolean,
  attemptCount: Int,
  processingTimeMs: Long,
  errorDetails: Option[String] = None
) extends TestMessage

// Enhanced Test Coordinator Actor
class EnhancedTestCoordinator(workers: List[ActorRef], config: EnhancedTestConfig, testRunId: String) extends Actor {
  implicit val log: LoggingAdapter = Logging(context.system, this)
  
  private var completedWorkers = 0
  private val totalWorkers = workers.size
  private var totalBatchesCompleted = 0
  private var totalRecordsProcessed = 0
  private var totalFailedBatches = 0
  private var totalRetries = 0
  private val failureTracking = scala.collection.mutable.Map[String, Int]()
  
  def receive: Receive = {
    case StartTest =>
      println(s"🎬 Enhanced Coordinator: Starting test with $totalWorkers workers")
      workers.foreach(_ ! ProcessBatchWithRetry(1, 1))
      
    case BatchCompletedWithDetails(workerId, batchNum, recordCount, success, attemptCount, processingTime, errorDetails) =>
      totalBatchesCompleted += 1
      
      if (success) {
        totalRecordsProcessed += recordCount
        if (attemptCount > 1) {
          totalRetries += (attemptCount - 1)
          println(s"🔄 $workerId batch $batchNum succeeded on attempt $attemptCount")
        }
      } else {
        totalFailedBatches += 1
        errorDetails.foreach { error =>
          failureTracking(error) = failureTracking.getOrElse(error, 0) + 1
        }
        
        // Retry logic
        if (attemptCount < config.retryAttempts) {
          val nextAttempt = attemptCount + 1
          println(s"🔄 $workerId batch $batchNum failed (attempt $attemptCount), retrying... (attempt $nextAttempt)")
          sender() ! ProcessBatchWithRetry(batchNum, nextAttempt)
          return
        } else {
          println(s"❌ $workerId batch $batchNum failed after $attemptCount attempts")
        }
      }
      
      if (batchNum < config.batchesPerWorker) {
        // Send next batch to this worker
        sender() ! ProcessBatchWithRetry(batchNum + 1, 1)
      } else {
        // Worker completed all batches
        sender() ! WorkerCompleted
      }
      
      // Enhanced progress update
      if (totalBatchesCompleted % 5 == 0) {
        val totalExpectedBatches = totalWorkers * config.batchesPerWorker
        val progress = (totalBatchesCompleted * 100.0) / totalExpectedBatches
        println(f"📈 Progress: $progress%.1f%% ($totalBatchesCompleted/$totalExpectedBatches batches, $totalRecordsProcessed records, $totalFailedBatches failed, $totalRetries retries)")
      }
      
    case WorkerCompleted =>
      completedWorkers += 1
      println(s"👷 Worker completed: $completedWorkers/$totalWorkers")
      
      if (completedWorkers == totalWorkers) {
        val totalExpectedBatches = totalWorkers * config.batchesPerWorker
        println(s"\n🏁 All workers completed!")
        println(s"📊 Final stats: $totalBatchesCompleted/$totalExpectedBatches batches, $totalRecordsProcessed records")
        println(s"📊 Failures: $totalFailedBatches failed batches, $totalRetries total retries")
        
        if (failureTracking.nonEmpty) {
          println("📊 Failure breakdown:")
          failureTracking.foreach { case (error, count) =>
            println(s"   🚨 $error: $count occurrences")
          }
        }
      }
  }
}

// Enhanced Batch Worker Actor  
class EnhancedBatchWorker(sessionInfo: EnhancedSessionInfo, config: EnhancedTestConfig, workerId: String) extends Actor {
  implicit val log: LoggingAdapter = Logging(context.system, this)
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val system: ActorSystem = context.system
  
  private val uploader = new AstraUploader()
  private val random = new Random()
  
  // Get the uploader's session ID for validation
  private val uploaderSessionId = try {
    // This would need to be implemented in AstraUploader to expose session info
    s"uploader_${uploader.hashCode()}"
  } catch {
    case _: Exception => "unknown_uploader_session"
  }
  
  // Prepare enhanced statement
  private val insertStmt: PreparedStatement = try {
    sessionInfo.session.prepare(
      """
      INSERT INTO enhanced_multi_session_test (
        id, batch_id, message_data, topic, session_id, worker_id, test_phase, 
        attempt_count, processing_start_time, processing_end_time, created_at, 
        uploader_session_id, error_details
      ) 
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, toTimestamp(now()), ?, ?)
      """
    )
  } catch {
    case ex: Exception =>
      log.error(ex, s"Failed to prepare enhanced statement for worker $workerId")
      throw ex
  }
  
  def receive: Receive = {
    case ProcessBatchWithRetry(batchNum, attemptCount) =>
      val startTime = System.currentTimeMillis()
      val batchId = UUID.randomUUID().toString
      val topic = s"topic_${sessionInfo.sessionType}_$batchNum"
      
      var errorDetails: Option[String] = None
      
      try {
        // Add some random delay to simulate real-world timing
        if (random.nextDouble() < 0.3) {
          Thread.sleep(random.nextInt(500) + 100) // 100-600ms delay
        }
        
        val processingStartTime = java.time.Instant.now()
        
        // Create enhanced batch elements
        val elements = (1 to config.recordsPerBatch).map { i =>
          val messageData = s"${workerId}_batch_${batchNum}_msg_$i"
          val boundStmt = insertStmt.bind(
            UUID.randomUUID(),
            batchId,
            messageData,
            topic,
            sessionInfo.sessionId,
            workerId,
            s"batch_$batchNum",
            java.lang.Integer.valueOf(attemptCount),
            java.time.Instant.now(),
            null, // processing_end_time - will be updated after processing
            uploaderSessionId,
            null  // error_details - only set on failure
          )
          (messageData, boundStmt)
        }
        
        val successfulBatch = Queue(elements.map(_._2): _*)
        
        // Apply chaos if this session has it
        sessionInfo.chaosWrapper.foreach { chaosWrapper =>
          if (random.nextDouble() < 0.5) { // 50% chance to change chaos level
            val newChaosLevel = random.nextDouble() * config.chaosLevel
            chaosWrapper.setChaosLevel(newChaosLevel)
          }
        }
        
        // Session validation
        if (config.sessionValidationEnabled) {
          // This would check if the session used by AstraUploader matches our session
          // Implementation would depend on AstraUploader internals
        }
        
        // Process batch using AstraUploader
        uploader.processBatch(elements, topic, successfulBatch)
        
        val processingEndTime = java.time.Instant.now()
        val processingDuration = processingEndTime.toEpochMilli() - processingStartTime.toEpochMilli()
        
        // Update processing end time in successful records
        // This would require a separate update operation in a real implementation
        
        val totalDuration = System.currentTimeMillis() - startTime
        
        // Simulate some batches taking longer (async processing)
        val asyncDelay = if (random.nextDouble() < 0.2) random.nextInt(2000) + 1000 else 500
        Thread.sleep(asyncDelay)
        
        if (config.detailedLogging) {
          println(s"✅ $workerId batch $batchNum (attempt $attemptCount) completed in ${totalDuration + asyncDelay}ms (processing: ${processingDuration}ms)")
        }
        
        sender() ! BatchCompletedWithDetails(workerId, batchNum, config.recordsPerBatch, success = true, attemptCount, totalDuration + asyncDelay)
        
      } catch {
        case ex: Exception =>
          val duration = System.currentTimeMillis() - startTime
          errorDetails = Some(ex.getClass.getSimpleName + ": " + ex.getMessage.take(100))
          
          log.error(ex, s"Enhanced batch processing failed for $workerId batch $batchNum attempt $attemptCount")
          
          if (config.detailedLogging) {
            println(s"❌ $workerId batch $batchNum (attempt $attemptCount) failed after ${duration}ms: ${ex.getMessage}")
          }
          
          sender() ! BatchCompletedWithDetails(workerId, batchNum, 0, success = false, attemptCount, duration, errorDetails)
      }
      
    case WorkerCompleted =>
      println(s"🏁 $workerId: All batches completed")
      context.stop(self)
  }
}