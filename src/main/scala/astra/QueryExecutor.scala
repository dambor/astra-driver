package astra

import java.util.concurrent.CompletionStage

import akka.event.LoggingAdapter
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, BoundStatement, Statement}

import scala.collection.mutable.Queue
import scala.concurrent.java8.FuturesConvertersImpl.{CF, P}
import scala.concurrent.{ExecutionContext, Future}

object QueryExecutor {

  // converting java CompletionStage => Scala Future
  private def toScalaFutures[T](cs: CompletionStage[T]): Future[T] = {
    cs match {
      case cf: CF[T] => cf.wrapped
      case _ =>
        val p = new P[T](cs)
        cs whenComplete p
        p.future
    }
  }

  def execute[T <: Statement[_]](statements: Queue[(T, CqlSession)])(cluster: BaseCluster)
                                (implicit ec: ExecutionContext, log: LoggingAdapter):
  Future[Queue[AsyncResultSet]] = {
    try {
      val futureList = statements.map({
        case (statement, session) => toScalaFutures(session.executeAsync(statement))
      })
      Future.sequence(futureList)
    } catch {
      case overflowExcep: ArrayIndexOutOfBoundsException =>
        log.error("Data overflow exception: " + overflowExcep.getMessage + "Will reset session and retry")
        Cluster.getOrCreateLiveSession.resetSession()
        Future.failed(overflowExcep)
      case e: Exception =>
        log.error("Error executing query: " + e.getMessage)
        Future.failed(e)
    }
  }

  def executeWithDefaultSession(statements: Queue[BoundStatement])(cluster: BaseCluster)
                               (implicit ec: ExecutionContext, log: LoggingAdapter):
  Future[Queue[AsyncResultSet]] = {
    try {
      val futureList = statements.map(statement => 
        toScalaFutures(Cluster.getOrCreateLiveSession.getSession().executeAsync(statement))
      )
      Future.sequence(futureList)
    } catch {
      case overflowExcep: ArrayIndexOutOfBoundsException =>
        log.error("Data overflow exception: " + overflowExcep.getMessage + "Will reset session and retry")
        Cluster.getOrCreateLiveSession.resetSession()
        Future.failed(overflowExcep)
      case e: Exception =>
        log.error("Error executing query: " + e.getMessage)
        Future.failed(e)
    }
  }
}