package examples

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import examples.DatabaseUsage

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}
import scala.concurrent.duration._
import scala.concurrent.Await

object UserManagerApp extends App {
  
  implicit val system: ActorSystem = ActorSystem("UserManagerSystem")
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val ec: ExecutionContext = system.dispatcher
  
  println("🚀 User Manager Application Starting...")
  
  val db = new DatabaseUsage()
  
  try {
    // === STEP 1: Setup Keyspace ===
    println("🏗️ Creating keyspace...")
    db.createKeyspaceIfNotExists("demo_keyspace")
    db.useKeyspace("demo_keyspace")
    
    // === STEP 2: Setup Tables ===
    println("📋 Setting up database tables...")
    db.createUserTable()
    
    // === STEP 3: Insert Some Users ===
    println("👥 Adding users...")
    
    db.insertUser(
      "550e8400-e29b-41d4-a716-446655440001", 
      "Alice Johnson", 
      "alice@example.com"
    )
    
    db.insertUser(
      "550e8400-e29b-41d4-a716-446655440002", 
      "Bob Smith", 
      "bob@example.com"
    )
    
    // === STEP 4: Batch Insert ===
    println("📦 Batch inserting users...")
    val batchUsers = List(
      ("550e8400-e29b-41d4-a716-446655440003", "Charlie Brown", "charlie@example.com"),
      ("550e8400-e29b-41d4-a716-446655440004", "Diana Prince", "diana@example.com"),
      ("550e8400-e29b-41d4-a716-446655440005", "Eve Wilson", "eve@example.com")
    )
    
    val batchFuture = db.insertMultipleUsers(batchUsers)
    Await.ready(batchFuture, 10.seconds)
    
    // === STEP 5: Query Users ===
    println("🔍 Querying users...")
    
    // Find specific user
    db.getUserByEmail("alice@example.com") match {
      case Some(row) => 
        println(s"Found user: ${row.getString("name")} (${row.getString("email")})")
      case None => 
        println("User not found")
    }
    
    // Get all users
    val allUsers = db.getAllUsers()
    println(s"📊 Total users in database: ${allUsers.length}")
    allUsers.foreach { user =>
      println(s"   • ${user.name} - ${user.email}")
    }
    
    // === STEP 6: Async Operations ===
    println("⚡ Testing async operations...")
    
    val asyncFuture = db.insertUserAsync(
      "550e8400-e29b-41d4-a716-446655440006",
      "Frank Miller",
      "frank@example.com"
    )
    
    asyncFuture.onComplete {
      case Success(_) => println("✅ Async insert completed successfully")
      case Failure(ex) => println(s"❌ Async insert failed: ${ex.getMessage}")
    }
    
    Await.ready(asyncFuture, 5.seconds)
    
    // === STEP 7: Error Handling Example ===
    println("🛡️ Testing error handling...")
    
    val errorTestFuture = db.safeInsertUser(
      "invalid-uuid", // This will cause an error
      "Test User",
      "test@example.com"
    )
    
    errorTestFuture.onComplete {
      case Success(Right(_)) => println("✅ Error test insert succeeded")
      case Success(Left(error)) => println(s"⚠️ Expected error caught: $error")
      case Failure(ex) => println(s"❌ Unexpected failure: ${ex.getMessage}")
    }
    
    Await.ready(errorTestFuture, 5.seconds)
    
    // === STEP 8: Simple Pagination Example ===
    println("📄 Testing pagination...")
    val limitedUsers = db.getUsersWithLimit(3)
    println(s"First 3 users:")
    limitedUsers.foreach(user => println(s"   • ${user.name}"))
    
    println("🎉 Application completed successfully!")
    
  } catch {
    case ex: Exception =>
      log.error(ex, "Application failed")
      println(s"❌ Application failed: ${ex.getMessage}")
      println("Make sure:")
      println("1. Your Astra credentials are set")
      println("2. Your keyspace exists")
      println("3. Your secure bundle path is correct")
  } finally {
    db.cleanup()
    system.terminate()
  }
}