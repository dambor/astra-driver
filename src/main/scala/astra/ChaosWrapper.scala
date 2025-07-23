package astra

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, PreparedStatement, ResultSet, Statement}

import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.util.Random

// Simple wrapper that doesn't extend CqlSession
class ChaosWrapper(realSession: CqlSession) {
  private val random = new Random()
  private var chaosLevel = 0.0
  private var isEnabled = false
  
  // Statistics
  private var totalRequests = 0
  private var chaosInjections = 0
  private var chaosTypeStats = Map[String, Int]().withDefaultValue(0)
  
  def setChaosLevel(level: Double): Unit = {
    chaosLevel = math.max(0.0, math.min(1.0, level))
    println(s"🐒 Chaos level set to ${(chaosLevel * 100).toInt}%")
  }
  
  def getChaosLevel: Double = chaosLevel  // Add getter method
  
  def enableChaos(): Unit = {
    isEnabled = true
    println("🐒 Chaos Monkey ENABLED")
  }
  
  def disableChaos(): Unit = {
    isEnabled = false
    println("🐒 Chaos Monkey DISABLED")
  }
  
  // Chaos-enabled async execution
  def executeAsync(statement: Statement[_]): CompletionStage[AsyncResultSet] = {
    totalRequests += 1
    
    if (isEnabled && random.nextDouble() < chaosLevel) {
      chaosInjections += 1
      injectChaos()
    } else {
      realSession.executeAsync(statement)
    }
  }
  
  // Direct delegation methods (no chaos)
  def execute(statement: Statement[_]): ResultSet = realSession.execute(statement)
  def prepare(query: String): PreparedStatement = realSession.prepare(query)
  def close(): Unit = realSession.close()
  
  private def injectChaos(): CompletionStage[AsyncResultSet] = {
    val chaosType = random.nextInt(3)
    chaosType match {
      case 0 =>
        chaosTypeStats = chaosTypeStats.updated("NetworkTimeout", chaosTypeStats("NetworkTimeout") + 1)
        println("🐒 Chaos: Injecting network timeout")
        CompletableFuture.failedFuture[AsyncResultSet](new RuntimeException("Chaos: Network timeout"))
        
      case 1 =>
        chaosTypeStats = chaosTypeStats.updated("ConnectionFailure", chaosTypeStats("ConnectionFailure") + 1)
        println("🐒 Chaos: Injecting connection failure")
        CompletableFuture.failedFuture[AsyncResultSet](new RuntimeException("Chaos: Connection unavailable"))
        
      case 2 =>
        chaosTypeStats = chaosTypeStats.updated("DatabaseError", chaosTypeStats("DatabaseError") + 1)
        println("🐒 Chaos: Injecting database error")
        CompletableFuture.failedFuture[AsyncResultSet](new RuntimeException("Chaos: Database internal error"))
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
       |  Breakdown: ${chaosTypeBreakdown.map { case (k, v) => s"$k=$v" }.mkString(", ")}""".stripMargin
  }
}