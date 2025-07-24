
package examples

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import astra.{AstraSession, AstraUploader, ChaosWrapper}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement, SimpleStatement}

import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * A custom, size-limited queue to simulate a fixed-size buffer that can overflow.
 *
 * @param maxSize The maximum number of elements the queue can hold.
 */
class BoundedQueue[A](maxSize: Int) extends mutable.Queue[A] {
  override def enqueue(elem: A): this.type = {
    if (size >= maxSize) {
      throw new IllegalStateException(s"Queue full (max size: $maxSize). Cannot add 1 more element.")
    }
    super.enqueue(elem)
    this
  }
}

object QueueFullChaosTest extends App {
  implicit val system: ActorSystem = ActorSystem("QueueFullChaosTestSystem")
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val ec: ExecutionContext = system.dispatcher

  println("🐒🚀 Testing AstraUploader with Queue Full Scenario...")

  val (realSession, chaosWrapper) = AstraSession.createSessionWithChaos("tradingtech")
  val tableName = "queue_full_chaos_test"

  try {
    setupTable(realSession, tableName)

    val insertStmt = realSession.prepare(
      s"""
      INSERT INTO $tableName (id, batch_id, message_data, topic, test_phase, created_at)
      VALUES (?, ?, ?, ?, ?, toTimestamp(now()))
      """
    )

    // Create a new AstraUploader that we can control
    val uploader = new ControllableAstraUploader()

    // Run the test
    testQueueOverflow(uploader, realSession, chaosWrapper, insertStmt, tableName)

  } catch {
    case ex: Exception =>
      log.error(ex, "Queue Full Chaos Test failed")
      println(s"❌ Test failed: ${ex.getMessage}")
      ex.printStackTrace()
  } finally {
    println("🔚 Terminating actor system...")
    realSession.close()
    system.terminate()
  }

  def setupTable(session: CqlSession, tableName: String): Unit = {
    println(s"Setting up table: $tableName")
    session.execute(
      s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        id UUID PRIMARY KEY,
        batch_id TEXT,
        message_data TEXT,
        topic TEXT,
        test_phase TEXT,
        created_at TIMESTAMP
      )
      """
    )
    println(s"✅ Table '$tableName' created or already exists.")
    session.execute(s"TRUNCATE $tableName")
    println(s"✅ Table '$tableName' truncated.")
  }

  def testQueueOverflow(
    uploader: ControllableAstraUploader,
    session: CqlSession,
    chaosWrapper: ChaosWrapper,
    insertStmt: PreparedStatement,
    tableName: String
  ): Unit = {
    val queueSize = 100
    val producerRate = 20 // Records per 100ms
    val batchId = UUID.randomUUID().toString
    val topic = "queue_full_topic"
    val testPhase = "QueueFull"

    // A BoundedQueue that will throw an exception when full
    val boundedQueue = new BoundedQueue[BoundStatement](queueSize)
    var recordsProduced = 0
    var recordsQueued = 0
    var recordsLost = 0

    println(s"""
=== Test: Queue Overflow Simulation ===
""")
    println(s"Queue size: $queueSize, Producer rate: $producerRate records/100ms")
    println("Simulating a 'stuck' consumer that doesn't process the batch...")

    // We "block" the uploader to ensure the queue fills up
    uploader.blockProcessing()

    // Producer loop: attempts to produce records faster than the queue can handle
    val producerFuture = Future {
      val totalRecordsToProduce = 200
      while (recordsProduced < totalRecordsToProduce) {
        Thread.sleep(100) // Produce records every 100ms

        val batchSize = producerRate
        val elements = (1 to batchSize).map { i =>
          val messageData = s"${testPhase}_message_${recordsProduced + i}"
          val boundStmt = insertStmt.bind(UUID.randomUUID(), batchId, messageData, topic, testPhase)
          (messageData, boundStmt)
        }

        println(s"PRODUCER: Attempting to queue $batchSize records. (Queue size: ${boundedQueue.size})")
        recordsProduced += batchSize

        try {
          elements.foreach { case (_, stmt) =>
            boundedQueue.enqueue(stmt)
            recordsQueued += 1
          }
          println(s"PRODUCER: Successfully queued $batchSize records. (New queue size: ${boundedQueue.size})")
        } catch {
          case e: IllegalStateException =>
            val lostCount = batchSize - (queueSize - (boundedQueue.size))
            recordsLost += lostCount
            println(s"PRODUCER: 💥 QUEUE FULL! Lost $lostCount records. ${e.getMessage}")
            // In a real scenario, these records are dropped. We stop producing.
            return
        }
      }
    }

    Await.result(producerFuture, 5.seconds)

    println("""PRODUCER: Finished producing records.""")
    println("UPLOADER: Releasing block and processing whatever is in the queue...")

    // Now, we "unblock" the uploader and let it process the batch
    val finalElements = boundedQueue.map(stmt => ("dummy_message", stmt)).toSeq
    uploader.unblockProcessing()
    uploader.processBatch(finalElements, topic, boundedQueue)

    // Wait for async operations to complete
    Thread.sleep(10000) // Allow time for the write to complete

    // Verification
    verifyResults(session, tableName, recordsQueued, recordsLost)
  }

  def verifyResults(session: CqlSession, tableName: String, recordsQueued: Int, recordsLost: Int): Unit = {
    println("""
=== Verification ===
""")
    val countResult = session.execute(s"SELECT COUNT(*) as count FROM $tableName").one()
    val recordsInDb = countResult.getLong("count")

    println(s"Records successfully queued: $recordsQueued")
    println(s"Records lost due to full queue: $recordsLost")
    println(s"Records actually written to DB: $recordsInDb")

    if (recordsInDb == recordsQueued && recordsLost > 0) {
      println(s"✅ SUCCESS: Test behaved as expected.")
      println(s"  - The $recordsInDb records that were successfully queued were written to the database.")
      println(s"  - The $recordsLost records that were produced while the queue was full were lost.")
    } else {
      println(s"🚨 FAILURE: Test did not behave as expected.")
      println(s"  - Expected to find $recordsQueued records in the DB, but found $recordsInDb.")
      if (recordsLost == 0) {
        println("  - The queue did not overflow as expected.")
      }
    }
  }

  class ControllableAstraUploader(implicit system: ActorSystem, log: LoggingAdapter) extends AstraUploader {
    private var processingBlocked = true
    private val latch = new CountDownLatch(1)

    def blockProcessing(): Unit = {
      processingBlocked = true
    }

    def unblockProcessing(): Unit = {
      processingBlocked = false
      latch.countDown()
    }

    override def processBatch(
      elements: Seq[(Any, BoundStatement)],
      topic: String,
      successfulBatch: mutable.Queue[BoundStatement]
    ): Unit = {
      if (processingBlocked) {
        println("UPLOADER: Processing is blocked. Waiting for unblock signal...")
        latch.await(10, TimeUnit.SECONDS)
      }

      println(s"UPLOADER: Processing batch of size ${successfulBatch.size} for topic '$topic'.")
      super.processBatch(elements, topic, successfulBatch)
    }
  }
}
