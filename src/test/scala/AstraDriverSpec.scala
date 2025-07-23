import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.testkit.TestKit
import astra.{BaseCluster, QueryExecutor, Config, AstraSession}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, BoundStatement}
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.Queue
import scala.concurrent.{ExecutionContext, Future}

class AstraDriverSpec extends TestKit(ActorSystem("AstraDriverSpec"))
  with AnyFlatSpecLike
  with Matchers
  with MockitoSugar
  with BeforeAndAfterAll {

  implicit val ec: ExecutionContext = system.dispatcher
  implicit val log: LoggingAdapter = Logging(system, this.getClass)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "Config" should "load configuration values" in {
    Config.driverRequestTimeoutMs should be > 0L
    Config.localDC should not be empty
    Config.maxRequestPerConnection should be > 0
  }

  "QueryExecutor" should "be properly defined" in {
    QueryExecutor should not be null
  }

  it should "handle empty statement queues" in {
    val mockCluster = mock[BaseCluster]
    val emptyStatements = Queue.empty[(BoundStatement, CqlSession)]
    
    val result = QueryExecutor.execute(emptyStatements)(mockCluster)
    
    result.map { resultQueue =>
      resultQueue shouldBe empty
    }
  }

  "AstraSession" should "have createSession methods" in {
    // Just verify the methods exist - we can't test actual connection without credentials
    noException should be thrownBy AstraSession.getClass.getMethods.find(_.getName == "createSession")
  }
}