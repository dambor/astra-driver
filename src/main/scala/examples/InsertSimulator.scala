package examples

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import astra.{AstraSession, QueryExecutor, Cluster}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{PreparedStatement, SimpleStatement, BoundStatement}
import com.datastax.oss.driver.api.core.servererrors.{WriteTimeoutException, UnavailableException}
import com.datastax.oss.driver.api.core.{AllNodesFailedException, DriverTimeoutException}

import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.collection.mutable.{Queue, ListBuffer}
import scala.util.{Success, Failure, Try}
import scala.util.Random
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.time.Duration

case class InsertResult(success: Boolean, id: UUID, error: Option[String] = None)
case class BatchResult(successCount: Int, failureCount: Int, errors: List[String])

object InsertSimulator extends App {
  
  implicit val system: ActorSystem = ActorSystem("InsertSimulatorSystem")
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val ec: ExecutionContext = system.dispatcher
  
  println("🚀 Insert Simulator Starting...")
  
  val targetKeyspace = "tradingtech"
  val maxRetries = 3
  val retryDelay = 1000 // ms
  val chaosMode = args.contains("--chaos") // Enable chaos mode with --chaos flag
  
  def executeWithRetry[T](operation: => T, description: String, retries: Int = maxRetries): Try[T] = {
    def attempt(remainingRetries: Int): Try[T] = {
      Try(operation) match {
        case Success(result) => Success(result)
        case Failure(ex) if remainingRetries > 0 =>
          ex match {
            case _: WriteTimeoutException | _: UnavailableException | _: DriverTimeoutException =>
              println(s"⚠️  $description failed (${ex.getClass.getSimpleName}), retrying in ${retryDelay}ms... ($remainingRetries retries left)")
              Thread.sleep(retryDelay)
              attempt(remainingRetries - 1)
            case _ =>
              println(s"❌ $description failed with non-retryable error: ${ex.getMessage}")
              Failure(ex)
          }
        case Failure(ex) =>
          println(s"❌ $description failed after all retries: ${ex.getMessage}")
          Failure(ex)
      }
    }
    attempt(retries)
  }
  
  def safeInsertUser(session: CqlSession, stmt: PreparedStatement, userId: UUID, username: String, email: String): InsertResult = {
    executeWithRetry(
      {
        val boundStmt = stmt.bind(userId, username, email)
        session.execute(boundStmt)
        boundStmt
      },
      s"User insert for $username"
    ) match {
      case Success(_) =>
        println(s"   ✓ User: $username ($email)")
        InsertResult(success = true, userId)
      case Failure(ex) =>
        println(s"   ❌ Failed to insert user $username: ${ex.getMessage}")
        InsertResult(success = false, userId, Some(ex.getMessage))
    }
  }
  
  def safeInsertOrder(session: CqlSession, stmt: PreparedStatement, orderId: UUID, userId: UUID, product: String, amount: BigDecimal): InsertResult = {
    executeWithRetry(
      {
        val boundStmt = stmt.bind(orderId, userId, product, amount.bigDecimal)
        session.execute(boundStmt)
        boundStmt
      },
      s"Order insert for $product"
    ) match {
      case Success(_) =>
        println(s"   ✓ Order: $product - $$$amount")
        InsertResult(success = true, orderId)
      case Failure(ex) =>
        println(s"   ❌ Failed to insert order $product: ${ex.getMessage}")
        InsertResult(success = false, orderId, Some(ex.getMessage))
    }
  }
  
  def simulateNetworkIssue(chaosMode: Boolean = false): Unit = {
    val issueChance = if (chaosMode) 0.3 else 0.1 // 30% vs 10% chance
    
    if (Random.nextDouble() < issueChance) {
      val issueType = Random.nextInt(3)
      issueType match {
        case 0 =>
          println("🌐 Simulating network delay...")
          Thread.sleep(Random.nextInt(3000) + 1000) // 1-4 second delay
        case 1 =>
          println("⚡ Simulating connection timeout...")
          Thread.sleep(Random.nextInt(8000) + 5000) // 5-13 second delay (likely to timeout)
        case 2 if chaosMode =>
          println("💥 Simulating connection failure...")
          throw new DriverTimeoutException("Simulated network failure")
        case _ =>
          // Fallback to delay
          Thread.sleep(Random.nextInt(2000) + 500)
      }
    }
  }
  
  def simulateInserts(session: CqlSession, keyspace: String): Unit = {
    println(s"📋 Setting up tables in keyspace: $keyspace...")
    
    // Create tables with better error handling
    val createUsersTable = SimpleStatement.newInstance(
      """
      CREATE TABLE IF NOT EXISTS users (
        user_id UUID PRIMARY KEY,
        username TEXT,
        email TEXT,
        created_at TIMESTAMP,
        last_login TIMESTAMP
      )
      """
    )
    
    val createOrdersTable = SimpleStatement.newInstance(
      """
      CREATE TABLE IF NOT EXISTS orders (
        order_id UUID PRIMARY KEY,
        user_id UUID,
        product_name TEXT,
        amount DECIMAL,
        order_date TIMESTAMP
      )
      """
    )
    
    try {
      session.execute(createUsersTable)
      session.execute(createOrdersTable)
      println("✅ Tables created successfully!")
    } catch {
      case ex: Exception =>
        println(s"❌ Failed to create tables: ${ex.getMessage}")
        return
    }
    
    // Prepare statements
    val insertUserStmt = session.prepare(
      "INSERT INTO users (user_id, username, email, created_at, last_login) VALUES (?, ?, ?, toTimestamp(now()), toTimestamp(now()))"
    )
    
    val insertOrderStmt = session.prepare(
      "INSERT INTO orders (order_id, user_id, product_name, amount, order_date) VALUES (?, ?, ?, ?, toTimestamp(now()))"
    )
    
    // Sample data
    val usernames = Array("alice", "bob", "charlie", "diana", "eve", "frank", "grace", "henry", "iris", "jack")
    val products = Array("Laptop", "Phone", "Tablet", "Headphones", "Mouse", "Keyboard", "Monitor", "Webcam", "Speaker", "Cable")
    val random = new Random()
    
    println("📝 Starting insert simulation with error handling...")
    
    // Track results
    val userResults = ListBuffer[InsertResult]()
    val orderResults = ListBuffer[InsertResult]()
    
    // Simulate user inserts with error tracking
    println("👥 Inserting users...")
    val userIds = (1 to 10).map { i =>
      val userId = UUID.randomUUID()
      val username = usernames(random.nextInt(usernames.length)) + i
      val email = s"$username@example.com"
      
      // Simulate potential network issues
      simulateNetworkIssue(chaosMode)
      
      val result = safeInsertUser(session, insertUserStmt, userId, username, email)
      userResults += result
      
      userId
    }.filter(_ => userResults.last.success) // Only include successful user IDs
    
    // Simulate order inserts with error tracking
    println("📦 Inserting orders...")
    if (userIds.nonEmpty) {
      (1 to 20).foreach { i =>
        val orderId = UUID.randomUUID()
        val userId = userIds(random.nextInt(userIds.length))
        val product = products(random.nextInt(products.length))
        val amount = BigDecimal((random.nextDouble() * 1000).round / 100.0)
        
        // Simulate potential network issues
        simulateNetworkIssue(chaosMode)
        
        val result = safeInsertOrder(session, insertOrderStmt, orderId, userId, product, amount)
        orderResults += result
      }
    } else {
      println("⚠️  No successful users inserted, skipping order inserts")
    }
    
    // Async batch insert simulation with better error handling
    println("⚡ Simulating async batch inserts...")
    
    val batchStatements = (1 to 5).map { i =>
      val userId = UUID.randomUUID()
      val username = s"batch_user_$i"
      val email = s"$username@batch.com"
      
      (insertUserStmt.bind(userId, username, email), session)
    }.to(Queue)
    
    val batchFuture = QueryExecutor.execute(batchStatements)(Cluster.getOrCreateLiveSession)
    
    batchFuture.onComplete {
      case Success(results) =>
        println(s"✅ Async batch completed: ${results.size} statements executed")
      case Failure(ex) =>
        println(s"❌ Async batch failed: ${ex.getMessage}")
        ex match {
          case _: AllNodesFailedException =>
            println("💡 All nodes failed - check cluster connectivity")
          case _: TimeoutException =>
            println("💡 Batch operation timed out - consider smaller batch sizes")
          case _ =>
            println(s"💡 Error type: ${ex.getClass.getSimpleName}")
        }
    }
    
    // Wait for async operations
    Thread.sleep(3000)
    
    // Query results with error handling
    println("🔍 Querying results...")
    
    try {
      val userCount = session.execute(SimpleStatement.newInstance("SELECT COUNT(*) FROM users")).one().getLong(0)
      val orderCount = session.execute(SimpleStatement.newInstance("SELECT COUNT(*) FROM orders")).one().getLong(0)
      
      println(s"📊 Final Statistics:")
      println(s"   • Total Users in DB: $userCount")
      println(s"   • Total Orders in DB: $orderCount")
      
      // Show success/failure statistics
      val successfulUsers = userResults.count(_.success)
      val failedUsers = userResults.count(!_.success)
      val successfulOrders = orderResults.count(_.success)
      val failedOrders = orderResults.count(!_.success)
      
      println(s"📈 Insert Statistics:")
      println(s"   • Successful User Inserts: $successfulUsers")
      println(s"   • Failed User Inserts: $failedUsers")
      println(s"   • Successful Order Inserts: $successfulOrders")
      println(s"   • Failed Order Inserts: $failedOrders")
      
      if (failedUsers > 0 || failedOrders > 0) {
        println("⚠️  Failed Insert Details:")
        userResults.filter(!_.success).foreach { result =>
          println(s"   • User ${result.id}: ${result.error.getOrElse("Unknown error")}")
        }
        orderResults.filter(!_.success).foreach { result =>
          println(s"   • Order ${result.id}: ${result.error.getOrElse("Unknown error")}")
        }
      }
      
      // Show some sample data if query succeeds
      val sampleUsers = session.execute(SimpleStatement.newInstance("SELECT username, email FROM users LIMIT 5"))
      println("👥 Sample Users:")
      sampleUsers.forEach { row =>
        println(s"   • ${row.getString("username")} - ${row.getString("email")}")
      }
      
      val sampleOrders = session.execute(SimpleStatement.newInstance("SELECT product_name, amount FROM orders LIMIT 5"))
      println("📦 Sample Orders:")
      sampleOrders.forEach { row =>
        println(s"   • ${row.getString("product_name")} - ${row.getBigDecimal("amount")}")
      }
      
    } catch {
      case ex: Exception =>
        println(s"❌ Failed to query results: ${ex.getMessage}")
        println("💡 The inserts may have succeeded even if querying failed")
    }
  }
  
  try {
    println(s"🔍 Connecting to keyspace: $targetKeyspace")
    val session = AstraSession.createSession(targetKeyspace)
    
    println(s"✅ Connected to keyspace: $targetKeyspace")
    simulateInserts(session, targetKeyspace)
    
    session.close()
    println("🎉 Insert simulation completed!")
    
  } catch {
    case ex: AllNodesFailedException =>
      log.error(ex, "All database nodes are unavailable")
      println(s"❌ Database connection failed: All nodes unavailable")
      println(s"💡 Check your network connection and Astra cluster status")
    case ex: Exception =>
      log.error(ex, "Simulation failed")
      println(s"❌ Simulation failed: ${ex.getMessage}")
      println(s"💡 Make sure the '$targetKeyspace' keyspace exists in your Astra database")
  } finally {
    system.terminate()
  }
}