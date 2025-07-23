package astra

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.BoundStatement

import scala.collection.mutable.Queue
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object AstraUploader {
  val SPLUNK_SEARCH_PREFIX = "[ASTRA_UPLOADER]"
}

/**
 * Example uploader class that handles batch processing with retry logic
 */
class AstraUploader(implicit system: ActorSystem, log: LoggingAdapter) {
  
  private implicit val responseExecutionContext: ExecutionContext = system.dispatcher
  
  @volatile private var failedToCommit: Boolean = false
  
  /**
   * Process a batch of statements with retry logic
   */
  def processBatch(elements: Seq[(Any, BoundStatement)], 
                   topic: String,
                   successfulBatch: Queue[BoundStatement]): Unit = {
    
    val flattenSuccessfulBatch: Queue[(BoundStatement, CqlSession)] = successfulBatch.map(stmt => (stmt, Cluster.getOrCreateLiveSession.getSession()))
    
    val opWriteF = QueryExecutor.execute(flattenSuccessfulBatch)(Cluster.getOrCreateLiveSession)(responseExecutionContext, log)

    opWriteF.onComplete {
      case Success(rs) =>
        if (failedToCommit) {
          failedToCommit = false
          log.info(
            s"${AstraUploader.SPLUNK_SEARCH_PREFIX} Successfully wrote previously failed messages received on " +
              s"topic=$topic to database"
          )
        }
        log.info(
          "{} processed batch of size={}. " +
            s"head:{}, tail:{}",
          AstraUploader.SPLUNK_SEARCH_PREFIX,
          successfulBatch.size,
          LBMMessageInfo.lbmMsgInfo(elements.head._1),
          LBMMessageInfo.lbmMsgInfo(elements.last._1)
        )
        // Move to next set of messages processing
        // Implementation specific logic here
        
      case Failure(e) =>
        if (failedToCommit == false) {
          failedToCommit = true
          log.warning(
            s"Failed to write ${successfulBatch.length} messages received on " +
              s"topic=$topic to database, " +
              s"reason=${e.getMessage}. Action will be retried."
          )
        }

        system.scheduler.scheduleOnce(Duration(1, TimeUnit.SECONDS)) {
          // Schedule reprocessing same batch
          processBatch(elements, topic, successfulBatch)
        }
    }
  }
}

/**
 * Placeholder for LBM message info extraction
 */
object LBMMessageInfo {
  def lbmMsgInfo(msg: Any): String = {
    // Implementation specific message info extraction
    msg.toString
  }
}