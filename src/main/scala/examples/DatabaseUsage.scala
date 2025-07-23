package examples

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import astra.{AstraSession, QueryExecutor, Cluster}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{PreparedStatement, SimpleStatement, BoundStatement, Row}

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.mutable.Queue
import scala.jdk.CollectionConverters._
import scala.util.{Success, Failure}

class DatabaseUsage(implicit system: ActorSystem, log: LoggingAdapter) {
  
  implicit val ec: ExecutionContext = system.dispatcher
  
  // Get session - connect without specifying keyspace first
  lazy val session: CqlSession = AstraSession.createSession()
  
  // === 1. SETUP AND KEYSPACE MANAGEMENT ===
  
  def createKeyspaceIfNotExists(keyspaceName: String): Unit = {
    val createKeyspace = SimpleStatement.newInstance(
      s"""
      CREATE KEYSPACE IF NOT EXISTS $keyspaceName 
      WITH REPLICATION = {
        'class': 'NetworkTopologyStrategy',
        'datacenter1': 1
      }
      """
    )
    session.execute(createKeyspace)
    log.info(s"Keyspace '$keyspaceName' created/verified")
  }
  
  def useKeyspace(keyspaceName: String): Unit = {
    val useStmt = SimpleStatement.newInstance(s"USE $keyspaceName")
    session.execute(useStmt)
    log.info(s"Now using keyspace: $keyspaceName")
  }
  
  def createUserTable(): Unit = {
    val createTable = SimpleStatement.newInstance(
      """
      CREATE TABLE IF NOT EXISTS users (
        user_id UUID PRIMARY KEY,
        name TEXT,
        email TEXT,
        created_at TIMESTAMP
      )
      """
    )
    session.execute(createTable)
    log.info("Users table created/verified")
  }
  
  def insertUser(userId: String, name: String, email: String): Unit = {
    val insert = SimpleStatement.newInstance(
      "INSERT INTO users (user_id, name, email, created_at) VALUES (?, ?, ?, toTimestamp(now()))",
      java.util.UUID.fromString(userId), name, email
    )
    session.execute(insert)
    log.info(s"Inserted user: $name")
  }
  
  def getUserByEmail(email: String): Option[Row] = {
    val select = SimpleStatement.newInstance(
      "SELECT * FROM users WHERE email = ? ALLOW FILTERING",
      email
    )
    val result = session.execute(select)
    Option(result.one())
  }
  
  // === 2. PREPARED STATEMENTS (Better Performance) ===
  
  lazy val insertUserPrepared: PreparedStatement = session.prepare(
    "INSERT INTO users (user_id, name, email, created_at) VALUES (?, ?, ?, toTimestamp(now()))"
  )
  
  def insertUserFast(userId: String, name: String, email: String): Unit = {
    val bound = insertUserPrepared.bind(
      java.util.UUID.fromString(userId), name, email
    )
    session.execute(bound)
    log.info(s"Fast insert user: $name")
  }
  
  // === 3. ASYNC OPERATIONS ===
  
  def insertUserAsync(userId: String, name: String, email: String): Future[Unit] = {
    val bound = insertUserPrepared.bind(
      java.util.UUID.fromString(userId), name, email
    )
    
    val statements = Queue((bound, session))
    QueryExecutor.execute(statements)(Cluster.getOrCreateLiveSession).map { results =>
      log.info(s"Async inserted user: $name")
    }
  }
  
  // === 4. BATCH OPERATIONS ===
  
  def insertMultipleUsers(users: List[(String, String, String)]): Future[Unit] = {
    val statements = users.map { case (userId, name, email) =>
      val bound = insertUserPrepared.bind(
        java.util.UUID.fromString(userId), name, email
      )
      (bound, session)
    }.to(Queue)
    
    QueryExecutor.execute(statements)(Cluster.getOrCreateLiveSession).map { results =>
      log.info(s"Batch inserted ${users.length} users")
    }
  }
  
  // === 5. QUERY WITH RESULTS PROCESSING ===
  
  def getAllUsers(): List[UserData] = {
    val select = SimpleStatement.newInstance("SELECT * FROM users")
    val result = session.execute(select)
    
    result.asScala.map { row =>
      UserData(
        userId = row.getUuid("user_id").toString,
        name = row.getString("name"),
        email = row.getString("email"),
        createdAt = Option(row.getInstant("created_at")).map(_.toString)
      )
    }.toList
  }
  
  // === 6. ERROR HANDLING ===
  
  def safeInsertUser(userId: String, name: String, email: String): Future[Either[String, Unit]] = {
    insertUserAsync(userId, name, email)
      .map(_ => Right(()))
      .recover {
        case ex: Exception => 
          log.error(ex, s"Failed to insert user: $name")
          Left(s"Insert failed: ${ex.getMessage}")
      }
  }
  
  // === 7. SIMPLE PAGINATION ===
  
  def getUsersPaginated(pageSize: Int = 10): List[UserData] = {
    val select = SimpleStatement.newInstance("SELECT * FROM users")
      .setPageSize(pageSize)
    
    val result = session.execute(select)
    
    // Get first page only for simplicity
    result.asScala.take(pageSize).map { row =>
      UserData(
        userId = row.getUuid("user_id").toString,
        name = row.getString("name"),
        email = row.getString("email"),
        createdAt = Option(row.getInstant("created_at")).map(_.toString)
      )
    }.toList
  }
  
  // Get users with LIMIT for simple pagination
  def getUsersWithLimit(limit: Int): List[UserData] = {
    val select = SimpleStatement.newInstance(s"SELECT * FROM users LIMIT $limit")
    val result = session.execute(select)
    
    result.asScala.map { row =>
      UserData(
        userId = row.getUuid("user_id").toString,
        name = row.getString("name"),
        email = row.getString("email"),
        createdAt = Option(row.getInstant("created_at")).map(_.toString)
      )
    }.toList
  }
  
  def cleanup(): Unit = {
    session.close()
  }
}

// Data model
case class UserData(
  userId: String,
  name: String,
  email: String,
  createdAt: Option[String]
)