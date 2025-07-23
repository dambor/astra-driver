package examples

import astra.AstraSession  // Import the existing AstraSession
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import java.util.UUID

object ChaosTest extends App {
  println("🐒 Starting Working Chaos Test...")
  
  try {
    // Step 1: Create session with chaos wrapper
    val (session, chaosWrapper) = AstraSession.createSessionWithChaos("tradingtech")
    println("✅ Session and chaos wrapper created successfully!")
    
    // Step 2: Create test table (using real session)  
    println("📋 Creating test table...")
    val createTable = SimpleStatement.newInstance(
      """
      CREATE TABLE IF NOT EXISTS chaos_test (
        id UUID PRIMARY KEY,
        test_name TEXT,
        data TEXT,
        created_at TIMESTAMP
      )
      """
    )
    session.execute(createTable)  // Use real session for table creation
    println("✅ Table created successfully!")
    
    // Step 3: Test without chaos (using real session)
    println("\n=== Test 1: Normal Operation (No Chaos) ===")
    chaosWrapper.disableChaos()
    
    val insertStmt = session.prepare(
      "INSERT INTO chaos_test (id, test_name, data, created_at) VALUES (?, ?, ?, toTimestamp(now()))"
    )
    
    // Insert a few records normally (using real session)
    (1 to 3).foreach { i =>
      val bound = insertStmt.bind(UUID.randomUUID(), "Normal", s"test_data_$i")
      session.execute(bound)  // Real session - no chaos
      println(s"✅ Inserted record $i normally")
    }
    
    // Step 4: Test with chaos enabled (using chaos wrapper)
    println("\n=== Test 2: With Chaos (50%) ===")
    chaosWrapper.enableChaos()
    chaosWrapper.setChaosLevel(0.5)
    
    // Try to insert records with chaos using executeAsync
    (1 to 8).foreach { i =>
      try {
        val bound = insertStmt.bind(UUID.randomUUID(), "Chaos", s"chaos_data_$i")
        
        // Use chaos wrapper's executeAsync (this can fail)
        val future = chaosWrapper.executeAsync(bound)
        
        // Convert to blocking call to see immediate result
        val result = future.toCompletableFuture.get()
        println(s"✅ Chaos record $i succeeded")
        
      } catch {
        case ex: Exception =>
          println(s"🐒 Chaos record $i failed: ${ex.getMessage}")
      }
    }
    
    // Step 5: Show statistics
    println("\n=== Final Statistics ===")
    println(chaosWrapper.getStats)
    
    session.close()
    println("🎉 Working chaos test completed!")
    
  } catch {
    case ex: Exception =>
      println(s"❌ Test failed: ${ex.getMessage}")
      ex.printStackTrace()
  }
}