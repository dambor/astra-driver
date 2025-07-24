package examples

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import astra._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException
import com.datastax.oss.driver.api.core.NoNodeAvailableException

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import java.util.UUID

object SpecificErrorChaosTest extends App {
  implicit val system: ActorSystem = ActorSystem("SpecificErrorChaosTestSystem")
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val ec: ExecutionContext = system.dispatcher

  println("💥 Starting Specific Error Injection Chaos Test...")

  var session: CqlSession = null
  try {
    val (realSession, chaosWrapper) = AstraSession.createSessionWithChaos("tradingtech")
    session = realSession
    println("✅ Session and chaos wrapper created.")

    setupTable(session)

    // --- Test Case: Injecting a Fatal Error ---
    println("\n=== Test Case: Injecting InvalidQueryException (Fatal) ===")
    
    // Configure chaos to throw a specific, non-retriable error
    val fatalError = new InvalidQueryException(null, "Chaos: This is a simulated fatal query error.")
    chaosWrapper.setChaosMode(astra.Failure)
    chaosWrapper.setFailureException(fatalError)
    chaosWrapper.setChaosLevel(0.5) // 50% chance to fail
    chaosWrapper.enableChaos()

    val uploader = new AstraUploader() // Assuming AstraUploader is part of the test
    val insertStmt = chaosWrapper.prepare("INSERT INTO specific_error_test (id, data) VALUES (?, ?)")

    println("📤 Attempting to insert a batch of 10 records, expecting failure...")
    val batch = (1 to 10).map(i => (s"data_$i", insertStmt.bind(UUID.randomUUID(), s"data_$i")))
    
    val result = Try {
      // This call should throw the InvalidQueryException
      uploader.processBatch(batch, "fatal_topic", scala.collection.mutable.Queue(batch.map(_._2): _*))
    }

    result match {
      case Failure(ex) if ex.isInstanceOf[InvalidQueryException] =>
        println(s"🎉 SUCCESS: Correctly caught the fatal error: ${ex.getClass.getSimpleName}")
        println("   - The application logic correctly propagated the non-retriable exception.")
      case Failure(ex) =>
        println(s"🚨 FAILURE: Caught an unexpected exception: ${ex.getClass.getSimpleName}")
        throw ex
      case Success(_) =>
        println("🚨 FAILURE: The batch processing succeeded when it should have failed.")
    }

    // --- Verification ---
    println("\n=== Verification ===")
    verifyNoRecordsInserted(session)

    println("\n=== Final Chaos Statistics ===")
    println(chaosWrapper.getStats)

  } catch {
    case ex: Exception =>
      log.error(ex, "Specific Error Chaos Test failed")
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
      CREATE TABLE IF NOT EXISTS specific_error_test (
        id UUID PRIMARY KEY,
        data TEXT
      )
      """
    ))
    session.execute(SimpleStatement.newInstance("TRUNCATE specific_error_test"))
    println("✅ Table '''specific_error_test''' is ready.")
  }

  def verifyNoRecordsInserted(session: CqlSession): Unit = {
    val count = session.execute("SELECT COUNT(*) as count FROM specific_error_test").one().getLong("count")
    println(s"   -> Verifying table contents: Found $count records.")
    if (count == 0) {
      println("🎉 SUCCESS: No records were inserted, as expected after a fatal error.")
    } else {
      println(s"🚨 FAILURE: Found $count records in the database. The application should have halted.")
    }
  }
}