package astra

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, PreparedStatement, ResultSet, Statement}

import java.util.concurrent.{CompletableFuture, CompletionStage, Executors, TimeUnit}
import scala.concurrent.duration.FiniteDuration
import scala.util.Random
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._

// Define Chaos Modes
sealed trait ChaosMode
case object Failure extends ChaosMode
case object Latency extends ChaosMode

class ChaosWrapper(realSession: CqlSession) {
  private val random = new Random()
  private var chaosLevel = 0.0
  private var isEnabled = false
  private var mode: ChaosMode = Failure // Default to failure mode
  private var latency: FiniteDuration = FiniteDuration(100, TimeUnit.MILLISECONDS)
  private var failureException: Throwable = new RuntimeException("Chaos: Default network timeout")

  // Statistics
  private var totalRequests = 0
  private var chaosInjections = 0
  private var chaosTypeStats = Map[String, Int]().withDefaultValue(0)

  // Scheduler for latency injection
  private val scheduler = Executors.newSingleThreadScheduledExecutor()

  def setChaosLevel(level: Double): Unit = {
    chaosLevel = math.max(0.0, math.min(1.0, level))
    println(s"🐒 Chaos level set to ${(chaosLevel * 100).toInt}%")
  }

  def getChaosLevel: Double = chaosLevel

  def setChaosMode(newMode: ChaosMode, newLatency: Option[FiniteDuration] = None): Unit = {
    mode = newMode
    newLatency.foreach(lat => latency = lat)
    println(s"🐒 Chaos mode set to $mode")
    if (mode == Latency) {
      println(s"   - Latency set to ${latency.toMillis} ms")
    }
  }

  def setFailureException(ex: Throwable): Unit = {
    failureException = ex
    println(s"🐒 Failure exception set to: ${ex.getClass.getSimpleName}")
  }

  def enableChaos(): Unit = {
    isEnabled = true
    println(s"🐒 Chaos Monkey ENABLED (Mode: $mode)")
  }

  def disableChaos(): Unit = {
    isEnabled = false
    println("🐒 Chaos Monkey DISABLED")
  }

  // Fixed: Now both async and sync methods go through chaos injection
  def executeAsync(statement: Statement[_]): CompletionStage[AsyncResultSet] = {
    totalRequests += 1

    if (isEnabled && random.nextDouble() < chaosLevel) {
      chaosInjections += 1
      injectChaosAsync(statement)
    } else {
      realSession.executeAsync(statement)
    }
  }

  // Fixed: Synchronous execute now also goes through chaos injection
  def execute(statement: Statement[_]): ResultSet = {
    totalRequests += 1

    if (isEnabled && random.nextDouble() < chaosLevel) {
      chaosInjections += 1
      injectChaosSync(statement)
    } else {
      realSession.execute(statement)
    }
  }

  // Fixed: Prepare statements now also tracked
  def prepare(query: String): PreparedStatement = {
    totalRequests += 1

    if (isEnabled && random.nextDouble() < chaosLevel) {
      chaosInjections += 1
      injectChaosPrepare(query)
    } else {
      realSession.prepare(query)
    }
  }

  def close(): Unit = {
    scheduler.shutdown()
    realSession.close()
  }

  // Async chaos injection
  private def injectChaosAsync(originalStatement: Statement[_]): CompletionStage[AsyncResultSet] = {
    mode match {
      case Failure =>
        injectFailureAsync()
      case Latency =>
        injectLatencyAsync(originalStatement)
    }
  }

  // Synchronous chaos injection
  private def injectChaosSync(originalStatement: Statement[_]): ResultSet = {
    mode match {
      case Failure =>
        injectFailureSync()
      case Latency =>
        injectLatencySync(originalStatement)
    }
  }

  // Prepare statement chaos injection
  private def injectChaosPrepare(query: String): PreparedStatement = {
    mode match {
      case Failure =>
        injectFailurePrepare()
      case Latency =>
        injectLatencyPrepare(query)
    }
  }

  // Async failure injection
  private def injectFailureAsync(): CompletionStage[AsyncResultSet] = {
    val errorType = failureException.getClass.getSimpleName
    chaosTypeStats = chaosTypeStats.updated(errorType, chaosTypeStats(errorType) + 1)
    println(s"🐒 Chaos: Injecting $errorType (async)")
    CompletableFuture.failedFuture[AsyncResultSet](failureException)
  }

  // Sync failure injection
  private def injectFailureSync(): ResultSet = {
    val errorType = failureException.getClass.getSimpleName
    chaosTypeStats = chaosTypeStats.updated(errorType, chaosTypeStats(errorType) + 1)
    println(s"🐒 Chaos: Injecting $errorType (sync)")
    throw failureException
  }

  // Prepare failure injection
  private def injectFailurePrepare(): PreparedStatement = {
    val errorType = failureException.getClass.getSimpleName
    chaosTypeStats = chaosTypeStats.updated(errorType, chaosTypeStats(errorType) + 1)
    println(s"🐒 Chaos: Injecting $errorType (prepare)")
    throw failureException
  }

  // Async latency injection
  private def injectLatencyAsync(originalStatement: Statement[_]): CompletionStage[AsyncResultSet] = {
    chaosTypeStats = chaosTypeStats.updated("Latency", chaosTypeStats("Latency") + 1)
    println(s"🐒 Chaos: Injecting ${latency.toMillis}ms latency (async)")
    val future = new CompletableFuture[AsyncResultSet]()
    
    scheduler.schedule(() => {
      realSession.executeAsync(originalStatement).whenComplete { (result, error) =>
        if (error != null) {
          future.completeExceptionally(error)
        } else {
          future.complete(result)
        }
      }
    }, latency.toMillis, TimeUnit.MILLISECONDS)
    
    future
  }

  // Sync latency injection
  private def injectLatencySync(originalStatement: Statement[_]): ResultSet = {
    chaosTypeStats = chaosTypeStats.updated("Latency", chaosTypeStats("Latency") + 1)
    println(s"🐒 Chaos: Injecting ${latency.toMillis}ms latency (sync)")
    
    try {
      Thread.sleep(latency.toMillis)
      realSession.execute(originalStatement)
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new RuntimeException("Chaos latency injection interrupted")
    }
  }

  // Prepare latency injection
  private def injectLatencyPrepare(query: String): PreparedStatement = {
    chaosTypeStats = chaosTypeStats.updated("Latency", chaosTypeStats("Latency") + 1)
    println(s"🐒 Chaos: Injecting ${latency.toMillis}ms latency (prepare)")
    
    try {
      Thread.sleep(latency.toMillis)
      realSession.prepare(query)
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new RuntimeException("Chaos latency injection interrupted")
    }
  }

  def getStats: ChaosStats = {
    ChaosStats(
      totalRequests = totalRequests,
      chaosInjections = chaosInjections,
      chaosRate = if (totalRequests > 0) chaosInjections.toDouble / totalRequests else 0.0,
      chaosTypeBreakdown = chaosTypeStats.toMap
    )
  }

  def resetStats(): Unit = {
    totalRequests = 0
    chaosInjections = 0
    chaosTypeStats = Map[String, Int]().withDefaultValue(0)
    println("🐒 Chaos statistics reset")
  }
}

case class ChaosStats(
  totalRequests: Int,
  chaosInjections: Int,
  chaosRate: Double,
  chaosTypeBreakdown: Map[String, Int]
) {
  override def toString: String = {
    s"""🐒 Chaos Monkey Statistics:
       |  Total Requests: $totalRequests
       |  Chaos Injections: $chaosInjections
       |  Chaos Rate: ${(chaosRate * 100).toInt}%
       |  Breakdown: ${chaosTypeBreakdown.map { case (k, v) => s"$k=$v" }.mkString(", ")}"""
  }
}