package examples

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import astra._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{PreparedStatement, SimpleStatement}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import java.util.UUID
import java.util.concurrent.CompletableFuture

object LatencyChaosTest extends App {
  implicit val system: ActorSystem = ActorSystem("LatencyChaosTestSystem")
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val ec: ExecutionContext = system.dispatcher

  val recordsToInsert = 500
  val latencyToInject = 100.milliseconds

  println("⏱️ Starting Latency Injection Chaos Test...")

  var session: CqlSession = null
  try {
    val (realSession, chaosWrapper) = AstraSession.createSessionWithChaos("tradingtech")
    session = realSession
    println("✅ Session and chaos wrapper created.")

    setupTable(session)

    // --- Warmup Phase ---
    warmup(chaosWrapper)

    // --- Phase 1: Baseline (No Chaos) ---
    println("\n=== Phase 1: Baseline Performance (No Chaos) ===")
    chaosWrapper.disableChaos()
    val baselineDuration = measureInsertionTime(chaosWrapper, recordsToInsert, "baseline")
    println(f"⏱️ Baseline completed in: $baselineDuration%.2f seconds")
    verify(session, recordsToInsert, "baseline")

    // --- Phase 2: Latency Injection --- 
    println("\n=== Phase 2: Latency Injection Test ===")
    chaosWrapper.resetStats()
    chaosWrapper.setChaosMode(Latency, Some(latencyToInject))
    chaosWrapper.setChaosLevel(1.0) // Inject latency on 100% of requests
    chaosWrapper.enableChaos()

    val latencyDuration = measureInsertionTime(chaosWrapper, recordsToInsert, "latency_test")
    println(f"⏱️ Latency test completed in: $latencyDuration%.2f seconds")
    verify(session, recordsToInsert, "latency_test")

    // --- Verification --- 
    println("\n=== Final Verification ===")
    val expectedMinDuration = baselineDuration + (latencyToInject.toSeconds * 0.8)
    println(f"- Baseline duration:       $baselineDuration%.2f s")
    println(f"- Latency duration:        $latencyDuration%.2f s")
    println(f"- Expected min duration:   $expectedMinDuration%.2f s (baseline + ~80%% of injected latency)")

    if (latencyDuration > baselineDuration && latencyDuration >= expectedMinDuration) {
      println("🎉 SUCCESS: Latency injection worked as expected.")
      println("   - The test with latency was significantly slower than the baseline.")
      println("   - All records were inserted correctly.")
    } else {
      println("🚨 FAILURE: Latency injection did not produce the expected slowdown.")
    }

    println("\n=== Final Chaos Statistics ===")
    println(chaosWrapper.getStats)

  } catch {
    case ex: Exception =>
      log.error(ex, "Latency Chaos Test failed")
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
    session.execute(SimpleStatement.newInstance(
      """
      CREATE TABLE IF NOT EXISTS latency_test (
        id UUID PRIMARY KEY,
        test_phase TEXT,
        created_at TIMESTAMP
      )
      """
    ))
    session.execute(SimpleStatement.newInstance("TRUNCATE latency_test"))
    println("✅ Table '''latency_test''' is ready.")
  }

  def warmup(chaosWrapper: ChaosWrapper): Unit = {
    println("\n=== Warming up JVM... ===")
    val insertStmt = chaosWrapper.prepare("INSERT INTO latency_test (id, test_phase, created_at) VALUES (?, ?, toTimestamp(now()))")
    val futures = (1 to 100).map {
      _ =>
        val boundStmt = insertStmt.bind(UUID.randomUUID(), "warmup")
        chaosWrapper.executeAsync(boundStmt).toCompletableFuture
    }
    CompletableFuture.allOf(futures: _*).get()
    // Clean up warmup data
    chaosWrapper.execute(SimpleStatement.newInstance("TRUNCATE latency_test"))
    println("   -> Warmup complete.")
  }

  def measureInsertionTime(chaosWrapper: ChaosWrapper, count: Int, testPhase: String): Double = {
    val insertStmt = chaosWrapper.prepare("INSERT INTO latency_test (id, test_phase, created_at) VALUES (?, ?, toTimestamp(now()))")
    val startTime = System.nanoTime()

    val futures = (1 to count).map {
      _ =>
        val boundStmt = insertStmt.bind(UUID.randomUUID(), testPhase)
        chaosWrapper.executeAsync(boundStmt).toCompletableFuture
    }

    // Wait for all insertions to complete
    val allFutures = CompletableFuture.allOf(futures: _*)
    allFutures.get() // Blocks until all are done

    val endTime = System.nanoTime()
    (endTime - startTime) / 1e9
  }

  def verify(session: CqlSession, expectedCount: Int, testPhase: String): Unit = {
    val query = SimpleStatement.builder("SELECT COUNT(*) as count FROM latency_test WHERE test_phase = ? ALLOW FILTERING")
      .addPositionalValue(testPhase)
      .build()
    val actualCount = session.execute(query).one().getLong("count")

    println(s"   -> Verifying phase '$testPhase': Expected $expectedCount, Found $actualCount")
    if (actualCount != expectedCount) {
      throw new RuntimeException(s"Verification failed for phase '$testPhase'! Missing records.")
    }
  }
}