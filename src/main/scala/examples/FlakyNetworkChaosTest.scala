package examples

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import astra.{AstraSession, AstraUploader, ChaosWrapper}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{SimpleStatement, PreparedStatement, BoundStatement}

import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import scala.util.{Random, Try, Success, Failure}
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.Queue

object FlakyNetworkChaosTest extends App {
  implicit val system: ActorSystem = ActorSystem("FlakyNetworkChaosTestSystem")
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val ec: ExecutionContext = system.dispatcher

  val recordsToInsert = 10000
  val testDuration = 2.minutes
  val testRunId = UUID.randomUUID().toString

  println("🌪️ Starting Flaky Network Chaos Test...")
  println(s"   - Test Run ID: $testRunId")
  println(s"   - Records to Insert: $recordsToInsert")
  println(s"   - Max Duration: ${testDuration.toSeconds} seconds")

  var session: CqlSession = null
  try {
    // 1. Setup Session and Table
    val (realSession, chaosWrapper) = AstraSession.createSessionWithChaos("tradingtech")
    session = realSession
    println("✅ Session and chaos wrapper created.")

    setupTable(session)

    // 2. Start the Flaky Network Simulator
    val keepSimulating = new AtomicBoolean(true)
    val simulatorFuture = startNetworkSimulator(chaosWrapper, keepSimulating)
    println("🌪️ Flaky network simulator started in the background.")

    // 3. Start Data Insertion
    println(s"📤 Starting to insert $recordsToInsert records...")
    val uploader = new AstraUploader()
    val insertStmt = session.prepare(
      """
      INSERT INTO flaky_network_test (id, test_run, record_index, created_at)
      VALUES (?, ?, ?, toTimestamp(now()))
      """
    )
    val insertionFuture = insertData(uploader, chaosWrapper, insertStmt, recordsToInsert, testRunId)

    // 4. Wait for completion and Verify
    Await.result(insertionFuture, testDuration)
    println("✅ Data insertion process finished.")

    // Stop the simulator
    keepSimulating.set(false)
    Await.result(simulatorFuture, 10.seconds)
    println("🛑 Flaky network simulator stopped.")

    // Final verification
    println("=== Verification ===")
    verify(session, recordsToInsert, testRunId)

    println("=== Final Chaos Statistics ===")
    println(chaosWrapper.getStats)

  } catch {
    case ex: Exception =>
      log.error(ex, "Flaky Network Chaos Test failed")
      println(s"❌ Test failed unexpectedly: ${ex.getMessage}")
      ex.printStackTrace()
  } finally {
    if (session != null) {
      session.close()
    }
    system.terminate()
    println("🔚 Test finished.")
  }

  def setupTable(session: CqlSession): Unit = {
    println("📋 Creating test table '''flaky_network_test'''...")
    session.execute(SimpleStatement.newInstance(
      """
      CREATE TABLE IF NOT EXISTS flaky_network_test (
        id UUID PRIMARY KEY,
        test_run TEXT,
        record_index INT,
        created_at TIMESTAMP
      )
      """
    ))
    println("🧹 Truncating table for a clean run...")
    session.execute(SimpleStatement.newInstance("TRUNCATE flaky_network_test"))
    println("✅ Table is ready.")
  }

  def startNetworkSimulator(chaosWrapper: ChaosWrapper, keepRunning: AtomicBoolean): Future[Unit] = {
    Future {
      var chaosEnabled = false
      while (keepRunning.get()) {
        Try {
          // Flip state every 50-250ms
          Thread.sleep(Random.nextInt(200) + 50)

          if (Random.nextDouble() < 0.6) { // 60% chance to flip
            chaosEnabled = !chaosEnabled
            if (chaosEnabled) {
              chaosWrapper.setChaosLevel(Random.nextDouble() * 0.7 + 0.1) // 10% - 80%
              chaosWrapper.enableChaos()
            } else {
              chaosWrapper.disableChaos()
            }
          }
        }
      }
      println("Simulator loop finished.")
    }
  }

  def insertData(
    uploader: AstraUploader,
    chaosWrapper: ChaosWrapper,
    insertStmt: PreparedStatement,
    count: Int,
    runId: String
  ): Future[Unit] = {
    Future {
      val batchSize = 100
      val numBatches = (count + batchSize - 1) / batchSize

      for (i <- 0 until numBatches) {
        val startIdx = i * batchSize
        val endIdx = Math.min((i + 1) * batchSize, count)
        val recordsInBatch = endIdx - startIdx

        val elements = (startIdx until endIdx).map { index =>
          val boundStmt = insertStmt.bind(UUID.randomUUID(), runId, java.lang.Integer.valueOf(index))
          (s"record_$index", boundStmt)
        }

        val successfulBatch = Queue(elements.map(_._2): _*)
        val topic = s"flaky_topic_batch_$i"

        // Use the uploader, which should handle retries internally
        uploader.processBatch(elements, topic, successfulBatch)
        
        if (i % 10 == 0) {
            println(s"  -> Sent batch ${i+1}/$numBatches (${recordsInBatch} records)")
        }
        // Small delay to allow network simulator to work its magic
        Thread.sleep(Random.nextInt(100) + 50)
      }
      println(s"✅ All $numBatches batches have been sent to the uploader.")
      // Give uploader time to process final batches and retries
      println("⏳ Waiting for final async uploads and retries to complete...")
      Thread.sleep(15000)
    }
  }

  def verify(session: CqlSession, expectedCount: Int, runId: String): Unit = {
    println(s"🔍 Verifying records for test run ID: $runId")
    val query = SimpleStatement.builder("SELECT COUNT(*) as count FROM flaky_network_test WHERE test_run = ? ALLOW FILTERING")
      .addPositionalValue(runId)
      .build()

    val resultSet = session.execute(query)
    val actualCount = resultSet.one().getLong("count")

    println(s"📊 Expected Records: $expectedCount")
    println(s"📊 Actual Records:   $actualCount")

    if (actualCount == expectedCount) {
      println(s"🎉 SUCCESS! All $expectedCount records were inserted correctly despite the flaky network.")
    } else {
      val difference = expectedCount - actualCount
      val lossPercentage = (difference.toDouble / expectedCount * 100).formatted("%.2f")
      println(s"🚨 FAILURE! Missing $difference records. Data loss: $lossPercentage%")
      println("🚨 This indicates the uploader'''s retry logic may not be resilient enough to rapid network changes.")
    }
  }
}
