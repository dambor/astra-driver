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
import java.text.NumberFormat
import java.util.Locale
import scala.util.{Random, Try}

object MultiSessionChaosTest extends App {
  implicit val system: ActorSystem = ActorSystem("MultiSessionChaosTestSystem")
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val ec: ExecutionContext = system.dispatcher
  
  // Parse command line arguments
  val config = parseArguments(args)
  
  println("🏭 Configurable Multi-Session Multi-Batch Chaos Test - Production Simulation")
  println("=" * 80)
  
  try {
    // Create test table
    val setupSession = Cluster.getOrCreateLiveSession.getSession()
    val createTable = SimpleStatement.newInstance(
      """
      CREATE TABLE IF NOT EXISTS multi_session_test (
        id UUID PRIMARY KEY,
        batch_id TEXT,
        message_data TEXT,
        topic TEXT,
        session_id TEXT,
        worker_id TEXT,
        test_phase TEXT,
        test_run_id TEXT,
        created_at TIMESTAMP
      )
      """
    )
    setupSession.execute(createTable)
    println("✅ Table created!")
    
    // CLEAN START (optional)
    if (!args.contains("--no-truncate")) {
      println("🧹 Clearing table...")
      val truncateTable = SimpleStatement.newInstance("TRUNCATE multi_session_test")
      setupSession.execute(truncateTable)
      
      // Verify clean start
      val initialCount = setupSession.execute(SimpleStatement.newInstance("SELECT COUNT(*) as count FROM multi_session_test")).one().getLong("count")
      println(s"📊 Initial count: $initialCount (should be 0)")
    }
    
    val testRunId = UUID.randomUUID().toString.take(8)
    println(s"🚀 Starting multi-session test (Test Run: $testRunId)")
    
    // Display test configuration
    printTestConfiguration(config)
    
    // Estimate completion time
    val estimatedDurationSeconds = estimateTestDuration(config)
    println(f"⏱️  Estimated completion time: ${estimatedDurationSeconds / 60}%.1f minutes")
    
    // Create multiple sessions (simulating different app instances)
    val sessions = createMultipleSessions(config.numSessions)
    println(s"✅ Created ${sessions.size} database sessions")
    
    // Create workers for each session
    val workers = createWorkers(sessions, config)
    println(s"✅ Created ${workers.size} worker actors")
    
    // Start the chaos test
    val coordinator = system.actorOf(Props(new TestCoordinator(workers, config, testRunId)), "coordinator")
    
    println("\n🎬 Starting multi-session chaos test...")
    val startTime = System.currentTimeMillis()
    coordinator ! StartTest
    
    // Wait for all workers to complete with dynamic timeout
    val maxWaitTimeMs = math.max(estimatedDurationSeconds * 1000 * 2, 300000) // At least 5 minutes
    println(s"⏳ Waiting up to ${maxWaitTimeMs / 60000} minutes for completion...")
    
    var lastProgressUpdate = System.currentTimeMillis()
    while (System.currentTimeMillis() - startTime < maxWaitTimeMs) {
      Thread.sleep(10000) // Check every 10 seconds
      
      // Print progress update every minute
      if (System.currentTimeMillis() - lastProgressUpdate > 60000) {
        val elapsedMinutes = (System.currentTimeMillis() - startTime) / 60000.0
        println(f"⏳ Still running... ${elapsedMinutes}%.1f minutes elapsed")
        lastProgressUpdate = System.currentTimeMillis()
      }
    }
    
    val totalDurationMinutes = (System.currentTimeMillis() - startTime) / 60000.0
    println(f"⏱️  Total test duration: ${totalDurationMinutes}%.1f minutes")
    
    // Verify results
    println("\n" + "=" * 80)
    println("🔍 FINAL VERIFICATION")
    println("=" * 80)
    
    val finalCount = setupSession.execute(SimpleStatement.newInstance("SELECT COUNT(*) as count FROM multi_session_test")).one().getLong("count")
    val expectedTotal = config.totalExpectedRecords
    
    println(s"📊 FINAL COUNT: $finalCount")
    println(s"📊 EXPECTED: $expectedTotal")
    
    if (finalCount == expectedTotal) {
      println(s"🎉 SUCCESS: All $expectedTotal records inserted correctly!")
      println(s"✅ Multi-session environment handles missing records correctly!")
    } else if (finalCount < expectedTotal) {
      println(s"🚨 MISSING RECORDS: Expected $expectedTotal, found $finalCount")
      println(s"🚨 Missing ${expectedTotal - finalCount} records")
      val lossPercentage = ((expectedTotal - finalCount).toDouble / expectedTotal * 100)
      println(f"🚨 Data loss: $lossPercentage%.2f%%")
      println(s"🚨 THIS REPRODUCES THE PRODUCTION BUG!")
    } else {
      println(s"🔄 EXTRA RECORDS: Expected $expectedTotal, found $finalCount") 
      println(s"🔄 Extra ${finalCount - expectedTotal} records - retry duplicates")
    }
    
    // Detailed breakdown
    analyzeResults(setupSession, config)
    
    // Cleanup
    sessions.foreach(_.session.close())
    
  } catch {
    case ex: Exception =>
      log.error(ex, "Multi-session chaos test failed")
      println(s"❌ Test failed: ${ex.getMessage}")
      ex.printStackTrace()
  } finally {
    println("🔚 Terminating actor system...")
    system.terminate()
  }
  
  def parseArguments(args: Array[String]): TestConfig = {
    var numSessions = 10
    var numWorkersPerSession = 10
    var batchesPerWorker = 10
    var recordsPerBatch = 1000
    var chaosLevel = 0.5
    
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--sessions" =>
          if (i + 1 < args.length) {
            numSessions = Try(args(i + 1).toInt).getOrElse(numSessions)
            i += 1
          }
        case "--workers" =>
          if (i + 1 < args.length) {
            numWorkersPerSession = Try(args(i + 1).toInt).getOrElse(numWorkersPerSession)
            i += 1
          }
        case "--batches" =>
          if (i + 1 < args.length) {
            batchesPerWorker = Try(args(i + 1).toInt).getOrElse(batchesPerWorker)
            i += 1
          }
        case "--records" =>
          if (i + 1 < args.length) {
            recordsPerBatch = Try(args(i + 1).toInt).getOrElse(recordsPerBatch)
            i += 1
          }
        case "--chaos" =>
          if (i + 1 < args.length) {
            chaosLevel = Try(args(i + 1).toDouble / 100.0).getOrElse(chaosLevel)
            i += 1
          }
        case "--help" =>
          printUsage()
          sys.exit(0)
        case _ =>
          // Ignore unknown arguments
      }
      i += 1
    }
    
    TestConfig(numSessions, numWorkersPerSession, batchesPerWorker, recordsPerBatch, chaosLevel)
  }
  
  def printUsage(): Unit = {
    println("Usage: sbt 'runMain examples.MultiSessionChaosTest [options]'")
    println()
    println("Options:")
    println("  --sessions N      Number of database sessions (default: 10)")
    println("  --workers N       Number of workers per session (default: 10)")
    println("  --batches N       Number of batches per worker (default: 10)")
    println("  --records N       Number of records per batch (default: 1000)")
    println("  --chaos N         Chaos level percentage 0-100 (default: 50)")
    println("  --no-truncate     Skip truncating the table before test")
    println("  --help            Show this help message")
    println()
    println("Examples:")
    println("  sbt 'runMain examples.MultiSessionChaosTest'")
    println("  sbt 'runMain examples.MultiSessionChaosTest --sessions 5 --chaos 30'")
    println("  sbt 'runMain examples.MultiSessionChaosTest --records 500 --batches 20'")
  }
  
  def printTestConfiguration(config: TestConfig): Unit = {
    println(s"📋 Test Configuration:")
    println(s"   • Sessions: ${config.numSessions}")
    println(s"   • Workers per session: ${config.numWorkersPerSession}")
    println(s"   • Batches per worker: ${config.batchesPerWorker}")
    println(s"   • Records per batch: ${config.recordsPerBatch}")
    println(s"   • Total expected records: ${NumberUtils.formatNumber(config.totalExpectedRecords)}")
    println(s"   • Chaos level: ${(config.chaosLevel * 100).toInt}%")
    println(s"   • Total workers: ${config.numSessions * config.numWorkersPerSession}")
    println(s"   • Total batches: ${NumberUtils.formatNumber(config.numSessions * config.numWorkersPerSession * config.batchesPerWorker)}")
  }
  
  def estimateTestDuration(config: TestConfig): Int = {
    // Rough estimation based on:
    // - ~50ms per record processing time
    // - Chaos causing retries (multiply by 1.5)
    // - Parallelism factor (divide by number of workers, but not linearly)
    val totalRecords = config.totalExpectedRecords
    val totalWorkers = config.numSessions * config.numWorkersPerSession
    val chaosMultiplier = 1.0 + (config.chaosLevel * 1.5) // More chaos = more retries
    val parallelismFactor = math.max(1.0, math.log10(totalWorkers.toDouble))
    
    val estimatedSeconds = (totalRecords * 0.05 * chaosMultiplier / parallelismFactor).toInt
    math.max(60, estimatedSeconds) // At least 1 minute
  }
  
  def createMultipleSessions(numSessions: Int): List[SessionInfo] = {
    (1 to numSessions).map { i =>
      // Create different types of sessions to simulate real production
      val sessionType = (i % 4) match {
        case 1 => 
          // Session type 1: Uses cluster session (like most of your app)
          val session = Cluster.getOrCreateLiveSession.getSession()
          SessionInfo(s"cluster_session_$i", session, "cluster")
        case 2 => 
          // Session type 2: Creates new session with keyspace (like some parts of your app)
          val session = AstraSession.createSession("tradingtech") 
          SessionInfo(s"direct_session_$i", session, "direct")
        case 3 =>
          // Session type 3: Creates session with chaos wrapper (like your tests)
          val (session, chaosWrapper) = AstraSession.createSessionWithChaos("tradingtech")
          chaosWrapper.enableChaos()
          chaosWrapper.setChaosLevel(0.3)
          SessionInfo(s"chaos_session_$i", session, "chaos", Some(chaosWrapper))
        case 0 =>
          // Session type 4: Mix of approaches
          val session = if (Random.nextBoolean()) {
            Cluster.getOrCreateLiveSession.getSession()
          } else {
            AstraSession.createSession("tradingtech")
          }
          SessionInfo(s"mixed_session_$i", session, "mixed")
      }
      
      if (i <= 10 || i % 10 == 0) { // Print first 10 and every 10th
        println(s"   📡 Created ${sessionType.sessionType} session: ${sessionType.sessionId}")
      }
      sessionType
    }.toList
  }
  
  def createWorkers(sessions: List[SessionInfo], config: TestConfig): List[ActorRef] = {
    val workers = scala.collection.mutable.ListBuffer[ActorRef]()
    
    sessions.foreach { sessionInfo =>
      (1 to config.numWorkersPerSession).foreach { workerNum =>
        val workerId = s"${sessionInfo.sessionId}_worker_$workerNum"
        val worker = system.actorOf(Props(new BatchWorker(sessionInfo, config, workerId)), workerId)
        workers += worker
      }
    }
    
    val totalWorkers = workers.size
    println(s"   👷 Created $totalWorkers workers total")
    workers.toList
  }
  
  def analyzeResults(session: CqlSession, config: TestConfig): Unit = {
    try {
      println("\n📊 Detailed Analysis:")
      
      // Breakdown by session type
      val sessionQuery = SimpleStatement.newInstance("SELECT session_id, COUNT(*) as count FROM multi_session_test GROUP BY session_id ALLOW FILTERING")
      val sessionResults = session.execute(sessionQuery)
      
      println("📡 Records by session (showing first 10):")
      var sessionCount = 0
      sessionResults.forEach { row =>
        if (sessionCount < 10) {
          val sessionId = row.getString("session_id")
          val count = row.getLong("count")
          val expectedPerSession = config.numWorkersPerSession * config.batchesPerWorker * config.recordsPerBatch
          val status = if (count >= expectedPerSession * 0.95) "✅" else "🚨"
          println(s"   $status $sessionId: $count/$expectedPerSession records")
          sessionCount += 1
        }
      }
      
      if (config.numSessions > 10) {
        println(s"   ... and ${config.numSessions - 10} more sessions")
      }
      
      // Summary statistics
      val distinctBatches = session.execute(SimpleStatement.newInstance("SELECT COUNT(DISTINCT batch_id) as batch_count FROM multi_session_test")).one().getLong("batch_count")
      val expectedBatches = config.numSessions * config.numWorkersPerSession * config.batchesPerWorker
      
      println(s"\n📦 Batch Analysis:")
      println(s"   • Expected batches: ${NumberUtils.formatNumber(expectedBatches)}")
      println(s"   • Found batches: ${NumberUtils.formatNumber(distinctBatches)}")
      
      if (distinctBatches < expectedBatches) {
        val missingBatches = expectedBatches - distinctBatches
        println(s"   🚨 Missing ${NumberUtils.formatNumber(missingBatches)} entire batches!")
        val missingPercentage = (missingBatches.toDouble / expectedBatches * 100)
        println(f"   🚨 Missing batch percentage: $missingPercentage%.2f%%")
      }
      
      // Performance metrics
      val avgRecordsPerBatch = if (distinctBatches > 0) {
        val totalRecords = session.execute(SimpleStatement.newInstance("SELECT COUNT(*) as total FROM multi_session_test")).one().getLong("total")
        totalRecords.toDouble / distinctBatches
      } else 0.0
      
      println(s"\n📈 Performance Metrics:")
      println(f"   • Average records per batch: $avgRecordsPerBatch%.1f (expected: ${config.recordsPerBatch})")
      
      if (avgRecordsPerBatch < config.recordsPerBatch * 0.95) {
        println("   🚨 Significantly fewer records per batch than expected!")
      }
      
    } catch {
      case ex: Exception =>
        println(s"⚠️ Could not analyze results: ${ex.getMessage}")
    }
  }
}

// Utility object for number formatting - accessible to all classes
object NumberUtils {
  private val numberFormat = NumberFormat.getNumberInstance(Locale.US)
  def formatNumber(num: Long): String = numberFormat.format(num)
  def formatNumber(num: Int): String = numberFormat.format(num)
}

// Data classes (unchanged)
case class TestConfig(
  numSessions: Int,
  numWorkersPerSession: Int,
  batchesPerWorker: Int,
  recordsPerBatch: Int,
  chaosLevel: Double
) {
  def totalExpectedRecords: Int = numSessions * numWorkersPerSession * batchesPerWorker * recordsPerBatch
}

case class SessionInfo(
  sessionId: String,
  session: CqlSession,
  sessionType: String,
  chaosWrapper: Option[astra.ChaosWrapper] = None
)

// Actor messages (unchanged)
sealed trait TestMessage
case object StartTest extends TestMessage
case class ProcessBatch(batchNum: Int) extends TestMessage
case class BatchCompleted(workerId: String, batchNum: Int, recordCount: Int, success: Boolean) extends TestMessage
case object WorkerCompleted extends TestMessage

// Test Coordinator Actor (enhanced for better progress tracking)
class TestCoordinator(workers: List[ActorRef], config: TestConfig, testRunId: String) extends Actor {
  implicit val log: LoggingAdapter = Logging(context.system, this)
  
  private var completedWorkers = 0
  private val totalWorkers = workers.size
  private var totalBatchesCompleted = 0
  private var totalRecordsProcessed = 0
  private var totalFailedBatches = 0
  private val startTime = System.currentTimeMillis()
  
  def receive: Receive = {
    case StartTest =>
      println(s"🎬 Coordinator: Starting test with $totalWorkers workers")
      println(s"🎯 Target: ${NumberUtils.formatNumber(config.totalExpectedRecords)} total records")
      workers.foreach(_ ! ProcessBatch(1))
      
    case BatchCompleted(workerId, batchNum, recordCount, success) =>
      totalBatchesCompleted += 1
      if (success) {
        totalRecordsProcessed += recordCount
      } else {
        totalFailedBatches += 1
      }
      
      if (batchNum < config.batchesPerWorker) {
        // Send next batch to this worker
        sender() ! ProcessBatch(batchNum + 1)
      } else {
        // Worker completed all batches
        sender() ! WorkerCompleted
      }
      
      // Enhanced progress update
      val progressInterval = if (config.totalExpectedRecords > 100000) 100 else 25
      if (totalBatchesCompleted % progressInterval == 0) {
        val totalExpectedBatches = totalWorkers * config.batchesPerWorker
        val progress = (totalBatchesCompleted * 100.0) / totalExpectedBatches
        val elapsedMinutes = (System.currentTimeMillis() - startTime) / 60000.0
        val recordsPerSecond = if (elapsedMinutes > 0) totalRecordsProcessed / (elapsedMinutes * 60) else 0
        
        println(f"📈 Progress: $progress%.1f%% (${NumberUtils.formatNumber(totalBatchesCompleted)}/${NumberUtils.formatNumber(totalExpectedBatches)} batches, ${NumberUtils.formatNumber(totalRecordsProcessed)} records, ${NumberUtils.formatNumber(totalFailedBatches)} failed) - ${elapsedMinutes}%.1f min elapsed - ${recordsPerSecond}%.0f rec/sec")
      }
      
    case WorkerCompleted =>
      completedWorkers += 1
      if (completedWorkers % 10 == 0 || completedWorkers == totalWorkers) {
        println(s"👷 Workers completed: $completedWorkers/$totalWorkers")
      }
      
      if (completedWorkers == totalWorkers) {
        val totalExpectedBatches = totalWorkers * config.batchesPerWorker
        val elapsedMinutes = (System.currentTimeMillis() - startTime) / 60000.0
        val avgRecordsPerSecond = if (elapsedMinutes > 0) totalRecordsProcessed / (elapsedMinutes * 60) else 0
        
        println(s"\n🏁 All workers completed!")
        println(f"📊 Final stats: ${NumberUtils.formatNumber(totalBatchesCompleted)}/${NumberUtils.formatNumber(totalExpectedBatches)} batches, ${NumberUtils.formatNumber(totalRecordsProcessed)} records, ${NumberUtils.formatNumber(totalFailedBatches)} failed")
        println(f"⏱️  Total time: ${elapsedMinutes}%.1f minutes")
        println(f"🚀 Average throughput: ${avgRecordsPerSecond}%.0f records/second")
      }
  }
}

// Batch Worker Actor (enhanced with better error handling)
class BatchWorker(sessionInfo: SessionInfo, config: TestConfig, workerId: String) extends Actor {
  implicit val log: LoggingAdapter = Logging(context.system, this)
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val system: ActorSystem = context.system
  
  private val uploader = new AstraUploader()
  private val random = new Random()
  private var processedBatches = 0
  
  // Prepare statement on this session (this might cause session mismatch!)
  private val insertStmt: PreparedStatement = try {
    sessionInfo.session.prepare(
      """
      INSERT INTO multi_session_test (id, batch_id, message_data, topic, session_id, worker_id, test_phase, test_run_id, created_at) 
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, toTimestamp(now()))
      """
    )
  } catch {
    case ex: Exception =>
      log.error(ex, s"Failed to prepare statement for worker $workerId")
      throw ex
  }
  
  def receive: Receive = {
    case ProcessBatch(batchNum) =>
      val startTime = System.currentTimeMillis()
      val batchId = UUID.randomUUID().toString
      val topic = s"topic_${sessionInfo.sessionType}_$batchNum"
      
      try {
        // Variable delay based on batch size to prevent overwhelming the system
        val delayMs = if (config.recordsPerBatch > 500) {
          random.nextInt(200) + 50 // 50-250ms for large batches
        } else {
          random.nextInt(100) + 25 // 25-125ms for smaller batches
        }
        Thread.sleep(delayMs)
        
        // Create batch elements
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
            sessionInfo.sessionId.take(8) // Use first 8 chars as test run ID
          )
          (messageData, boundStmt)
        }
        
        val successfulBatch = Queue(elements.map(_._2): _*)
        
        // Apply chaos if this session has it
        sessionInfo.chaosWrapper.foreach { chaosWrapper =>
          if (random.nextDouble() < 0.3) { // 30% chance to change chaos level
            val newChaosLevel = random.nextDouble() * config.chaosLevel
            chaosWrapper.setChaosLevel(newChaosLevel)
          }
        }
        
        // Process batch using AstraUploader
        uploader.processBatch(elements, topic, successfulBatch)
        
        val duration = System.currentTimeMillis() - startTime
        processedBatches += 1
        
        // Longer wait for larger batches or chaos
        val asyncDelay = if (config.recordsPerBatch > 500) {
          if (sessionInfo.chaosWrapper.isDefined) {
            random.nextInt(3000) + 2000 // 2-5 seconds with chaos
          } else {
            random.nextInt(2000) + 1000 // 1-3 seconds without chaos
          }
        } else {
          random.nextInt(1000) + 500 // 0.5-1.5 seconds for smaller batches
        }
        Thread.sleep(asyncDelay)
        
        // Only log every 10th batch to reduce noise
        if (batchNum % 10 == 0 || batchNum == config.batchesPerWorker) {
          println(s"✅ $workerId: batch $batchNum/${config.batchesPerWorker} completed (${config.recordsPerBatch} records, ${duration + asyncDelay}ms)")
        }
        
        sender() ! BatchCompleted(workerId, batchNum, config.recordsPerBatch, success = true)
        
      } catch {
        case ex: Exception =>
          val duration = System.currentTimeMillis() - startTime
          log.error(ex, s"Batch processing failed for $workerId batch $batchNum")
          println(s"❌ $workerId batch $batchNum failed after ${duration}ms: ${ex.getMessage}")
          
          sender() ! BatchCompleted(workerId, batchNum, 0, success = false)
      }
      
    case WorkerCompleted =>
      println(s"🏁 $workerId: All ${config.batchesPerWorker} batches completed")
      context.stop(self)
  }
}