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
import scala.util.Random

object MultiSessionChaosTest extends App {
  implicit val system: ActorSystem = ActorSystem("MultiSessionChaosTestSystem")
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val ec: ExecutionContext = system.dispatcher
  
  println("🏭 Multi-Session Multi-Batch Chaos Test - Production Simulation")
  println("=" * 70)
  
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
        created_at TIMESTAMP
      )
      """
    )
    setupSession.execute(createTable)
    println("✅ Table created!")
    
    // CLEAN START
    println("🧹 Clearing table...")
    val truncateTable = SimpleStatement.newInstance("TRUNCATE multi_session_test")
    setupSession.execute(truncateTable)
    
    // Verify clean start
    val initialCount = setupSession.execute(SimpleStatement.newInstance("SELECT COUNT(*) as count FROM multi_session_test")).one().getLong("count")
    println(s"📊 Initial count: $initialCount (should be 0)")
    
    val testRunId = UUID.randomUUID().toString.take(8)
    println(s"🚀 Starting multi-session test (Test Run: $testRunId)")
    
    // Test Configuration
    val testConfig = TestConfig(
      numSessions = 3,           // 3 different database sessions
      numWorkersPerSession = 2,  // 2 workers per session
      batchesPerWorker = 5,      // 5 batches per worker
      recordsPerBatch = 100,     // 100 records per batch
      chaosLevel = 0.3          // 30% chaos
    )
    
    println(s"📋 Test Configuration:")
    println(s"   • Sessions: ${testConfig.numSessions}")
    println(s"   • Workers per session: ${testConfig.numWorkersPerSession}")
    println(s"   • Batches per worker: ${testConfig.batchesPerWorker}")
    println(s"   • Records per batch: ${testConfig.recordsPerBatch}")
    println(s"   • Total expected records: ${testConfig.totalExpectedRecords}")
    println(s"   • Chaos level: ${(testConfig.chaosLevel * 100).toInt}%")
    
    // Create multiple sessions (simulating different app instances)
    val sessions = createMultipleSessions(testConfig.numSessions)
    println(s"✅ Created ${sessions.size} database sessions")
    
    // Create workers for each session
    val workers = createWorkers(sessions, testConfig)
    println(s"✅ Created ${workers.size} worker actors")
    
    // Start the chaos test
    val coordinator = system.actorOf(Props(new TestCoordinator(workers, testConfig, testRunId)), "coordinator")
    
    println("\n🎬 Starting multi-session chaos test...")
    coordinator ! StartTest
    
    // Wait for all workers to complete
    Thread.sleep(120000) // 2 minutes for completion
    
    // Verify results
    println("\n" + "=" * 70)
    println("🔍 FINAL VERIFICATION")
    println("=" * 70)
    
    val finalCount = setupSession.execute(SimpleStatement.newInstance("SELECT COUNT(*) as count FROM multi_session_test")).one().getLong("count")
    val expectedTotal = testConfig.totalExpectedRecords
    
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
    analyzeResults(setupSession, testConfig)
    
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
  
  def createMultipleSessions(numSessions: Int): List[SessionInfo] = {
    (1 to numSessions).map { i =>
      // Create different types of sessions to simulate real production
      val sessionType = i match {
        case 1 => 
          // Session 1: Uses cluster session (like most of your app)
          val session = Cluster.getOrCreateLiveSession.getSession()
          SessionInfo(s"cluster_session_$i", session, "cluster")
        case 2 => 
          // Session 2: Creates new session with keyspace (like some parts of your app)
          val session = AstraSession.createSession("tradingtech") 
          SessionInfo(s"direct_session_$i", session, "direct")
        case 3 =>
          // Session 3: Creates session with chaos wrapper (like your tests)
          val (session, chaosWrapper) = AstraSession.createSessionWithChaos("tradingtech")
          chaosWrapper.enableChaos()
          chaosWrapper.setChaosLevel(0.3)
          SessionInfo(s"chaos_session_$i", session, "chaos", Some(chaosWrapper))
        case _ =>
          // Additional sessions: Mix of approaches
          val session = if (Random.nextBoolean()) {
            Cluster.getOrCreateLiveSession.getSession()
          } else {
            AstraSession.createSession("tradingtech")
          }
          SessionInfo(s"mixed_session_$i", session, "mixed")
      }
      
      println(s"   📡 Created ${sessionType.sessionType} session: ${sessionType.sessionId}")
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
        println(s"   👷 Created worker: $workerId")
      }
    }
    
    workers.toList
  }
  
  def analyzeResults(session: CqlSession, config: TestConfig): Unit = {
    try {
      println("\n📊 Detailed Analysis:")
      
      // Breakdown by session type
      val sessionQuery = SimpleStatement.newInstance("SELECT session_id, COUNT(*) as count FROM multi_session_test GROUP BY session_id ALLOW FILTERING")
      val sessionResults = session.execute(sessionQuery)
      
      println("📡 Records by session:")
      sessionResults.forEach { row =>
        val sessionId = row.getString("session_id")
        val count = row.getLong("count")
        val expectedPerSession = config.numWorkersPerSession * config.batchesPerWorker * config.recordsPerBatch
        val status = if (count == expectedPerSession) "✅" else "🚨"
        println(s"   $status $sessionId: $count/$expectedPerSession records")
      }
      
      // Breakdown by worker
      val workerQuery = SimpleStatement.newInstance("SELECT worker_id, COUNT(*) as count FROM multi_session_test GROUP BY worker_id ALLOW FILTERING")
      val workerResults = session.execute(workerQuery)
      
      println("\n👷 Records by worker:")
      workerResults.forEach { row =>
        val workerId = row.getString("worker_id")
        val count = row.getLong("count")
        val expectedPerWorker = config.batchesPerWorker * config.recordsPerBatch
        val status = if (count == expectedPerWorker) "✅" else "🚨"
        println(s"   $status $workerId: $count/$expectedPerWorker records")
      }
      
      // Look for patterns in missing records
      val distinctBatches = session.execute(SimpleStatement.newInstance("SELECT COUNT(DISTINCT batch_id) as batch_count FROM multi_session_test")).one().getLong("batch_count")
      val expectedBatches = config.numSessions * config.numWorkersPerSession * config.batchesPerWorker
      
      println(s"\n📦 Batch Analysis:")
      println(s"   • Expected batches: $expectedBatches")
      println(s"   • Found batches: $distinctBatches")
      
      if (distinctBatches < expectedBatches) {
        println(s"   🚨 Missing ${expectedBatches - distinctBatches} entire batches!")
      }
      
    } catch {
      case ex: Exception =>
        println(s"⚠️ Could not analyze results: ${ex.getMessage}")
    }
  }
}

// Data classes
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

// Actor messages
sealed trait TestMessage
case object StartTest extends TestMessage
case class ProcessBatch(batchNum: Int) extends TestMessage
case class BatchCompleted(workerId: String, batchNum: Int, recordCount: Int, success: Boolean) extends TestMessage
case object WorkerCompleted extends TestMessage

// Test Coordinator Actor
class TestCoordinator(workers: List[ActorRef], config: TestConfig, testRunId: String) extends Actor {
  implicit val log: LoggingAdapter = Logging(context.system, this)
  
  private var completedWorkers = 0
  private val totalWorkers = workers.size
  private var totalBatchesCompleted = 0
  private var totalRecordsProcessed = 0
  private var totalFailedBatches = 0
  
  def receive: Receive = {
    case StartTest =>
      println(s"🎬 Coordinator: Starting test with $totalWorkers workers")
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
      
      // Progress update
      if (totalBatchesCompleted % 10 == 0) {
        val totalExpectedBatches = totalWorkers * config.batchesPerWorker
        val progress = (totalBatchesCompleted * 100.0) / totalExpectedBatches
        println(f"📈 Progress: $progress%.1f%% ($totalBatchesCompleted/$totalExpectedBatches batches, $totalRecordsProcessed records, $totalFailedBatches failed)")
      }
      
    case WorkerCompleted =>
      completedWorkers += 1
      println(s"👷 Worker completed: $completedWorkers/$totalWorkers")
      
      if (completedWorkers == totalWorkers) {
        val totalExpectedBatches = totalWorkers * config.batchesPerWorker
        println(s"\n🏁 All workers completed!")
        println(s"📊 Final stats: $totalBatchesCompleted/$totalExpectedBatches batches, $totalRecordsProcessed records, $totalFailedBatches failed")
      }
  }
}

// Batch Worker Actor
class BatchWorker(sessionInfo: SessionInfo, config: TestConfig, workerId: String) extends Actor {
  implicit val log: LoggingAdapter = Logging(context.system, this)
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val system: ActorSystem = context.system
  
  private val uploader = new AstraUploader()
  private val random = new Random()
  
  // Prepare statement on this session (this might cause session mismatch!)
  private val insertStmt: PreparedStatement = try {
    sessionInfo.session.prepare(
      """
      INSERT INTO multi_session_test (id, batch_id, message_data, topic, session_id, worker_id, test_phase, created_at) 
      VALUES (?, ?, ?, ?, ?, ?, ?, toTimestamp(now()))
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
        // Add some random delay to simulate real-world timing
        if (random.nextDouble() < 0.3) {
          Thread.sleep(random.nextInt(500) + 100) // 100-600ms delay
        }
        
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
            s"batch_$batchNum"
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
        
        // Process batch using AstraUploader
        uploader.processBatch(elements, topic, successfulBatch)
        
        val duration = System.currentTimeMillis() - startTime
        
        // Simulate some batches taking longer (async processing)
        val asyncDelay = if (random.nextDouble() < 0.2) random.nextInt(2000) + 1000 else 500
        Thread.sleep(asyncDelay)
        
        println(s"✅ $workerId batch $batchNum completed in ${duration + asyncDelay}ms (${config.recordsPerBatch} records)")
        
        sender() ! BatchCompleted(workerId, batchNum, config.recordsPerBatch, success = true)
        
      } catch {
        case ex: Exception =>
          val duration = System.currentTimeMillis() - startTime
          log.error(ex, s"Batch processing failed for $workerId batch $batchNum")
          println(s"❌ $workerId batch $batchNum failed after ${duration}ms: ${ex.getMessage}")
          
          sender() ! BatchCompleted(workerId, batchNum, 0, success = false)
      }
      
    case WorkerCompleted =>
      println(s"🏁 $workerId: All batches completed")
      context.stop(self)
  }
}