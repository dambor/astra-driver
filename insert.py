// Astra Driver configuration


package astra

import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverConfigLoader}
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy
import com.datastax.oss.driver.internal.core.tracker.RequestLogger


trait SessionConfig {

  val loader = DriverConfigLoader.programmaticBuilder()

  /**
   *
   * set socket options
   */
    .withBoolean(DefaultDriverOption.SOCKET_KEEP_ALIVE, true)
    .withBoolean(DefaultDriverOption.SOCKET_TCP_NODELAY, true)

    /**
     * set request settings
     */
    .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM")
    .withBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE, true)
    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(Config.driverRequestTimeoutMs))

    /**
     * set load balancing option
     * Default policy
     * In previous versions, the driver provided a wide variety of built-in load balancing policies;
     * in addition, they could be nested into each other, yielding an even higher number of choices.
     * In our experience, this has proven to be too complicated: it's not obvious which policy(ies)
     * to choose for a given use case, and nested policies can sometimes affect each other's
     * effects in subtle and hard to predict ways.
     *
     * In driver 4+, we are taking a more opinionated approach: we provide a single
     * load balancing policy, that we consider the best choice for most cases.
     * You can still write a custom implementation if you have special requirements.
     */
    .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, Config.localDC)
    .withClass(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, classOf[DefaultLoadBalancingPolicy])

    /**
     * set Pooling options
     */
    .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, Config.maxRequestPerConnection)
    .withInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_QUEUE_SIZE, Config.maxQueueSize)
    .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, Config.maxLocalConnectionPerhost)


    /**
    * Enabling Metrics for cassandra
    * Session
    * 1. cql-requests
    * 2. cql-client-timeouts
    *
    * Node
    * 1. pool.open-connections
    * 2. pool.in-flight
    * 3. errors.request.write-timeouts
    * 4. errors.request.read-timeouts
    * 5. retries.write-timeout
    * 6. retries.read-timeout
    */
    .withString(DefaultDriverOption.METRICS_FACTORY_CLASS,"com.datastax.oss.driver.internal.core.metrics.DefaultMetricsFactory")
    .withStringList(DefaultDriverOption.METRICS_SESSION_ENABLED, List[String]("cql-requests", "cql-client-timeouts"))
    .withStringList(DefaultDriverOption.METRICS_NODE_ENABLED, List[String]( "pool.open-connections", "pool.in-flight",
                                                                            "errors.request.write-timeouts","errors.request.read-timeouts",
                                                                            "retries.write-timeout","retries.read-timeout"))

    /**
     * enable warning logs
     */
    .withBoolean(DefaultDriverOption.REQUEST_LOG_WARNINGS, true)

    /** Request tracking logging enabled
     * INFO  c.d.o.d.i.core.tracker.RequestLogger - [s0][/127.0.0.1:9042] Success (13 ms) [1 values]
     * SELECT * FROM users WHERE user_id=? [v0=42]
     *
     * INFO  c.d.o.d.i.core.tracker.RequestLogger - [s0][/127.0.0.1:9042] Slow (1.245 s) [1 values] SELECT
     * * FROM users WHERE user_id=? [v0=42]
     * Failed requests use the ERROR level:
     *
     * ERROR c.d.o.d.i.core.tracker.RequestLogger - [s0][/127.0.0.1:9042] Error (179 ms) [1 values] SELECT
     * all FROM users WHERE user_id=? [v0=42]
     * com.datastax.oss.driver.api.core.servererrors.InvalidQueryException: Undefined column name all
     */
    .withClass(DefaultDriverOption.REQUEST_TRACKER_CLASS, classOf[RequestLogger])
    .withBoolean(DefaultDriverOption.REQUEST_LOGGER_SLOW_ENABLED, true)
    .withBoolean(DefaultDriverOption.REQUEST_LOGGER_ERROR_ENABLED, true)
    .withBoolean(DefaultDriverOption.REQUEST_LOGGER_SUCCESS_ENABLED, false)

    /**
     * compression of requests and responses to reduce network traffic at the cost of slight CPU overhead
     *
     * It require additional third party dependency
     * // https://mvnrepository.com/artifact/org.xerial.snappy/snappy-java
     * libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.9.1"
     */
    //.withString(DefaultDriverOption.PROTOCOL_COMPRESSION,"snappy")

    .endProfile()
    .build()
}




// session creation
      val astraSession = CqlSession.builder()
        .withConfigLoader(loader)
        .withCloudSecureConnectBundle(Paths.get(Config.secureConnectionBundlePath))
        .withAuthCredentials(getClientId(), getClientSecret())
        .withKeyspace(keyspace)
        .build()


// Query Executor class


package astra

import java.util.concurrent.CompletionStage

import akka.event.LoggingAdapter
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, BoundStatement, ResultSet, Statement}

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

  def execute[T <: Statement[_]] (statements: Queue[(T, CqlSession)])(cluster: BaseCluster)
             (implicit ec: ExecutionContext, log:LoggingAdapter):
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
                               (implicit ec: ExecutionContext, log:LoggingAdapter) :
  Future[Queue[AsyncResultSet]] = {
    try{
      val futureList = statements.map(statement =>toScalaFutures(Cluster.getOrCreateLiveSession.getSession().executeAsync(statement)))
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

Application Code to Retry in case of failure 

    val opWriteF = QueryExecutor.execute(flattenSuccessfulBatch)(AstraCluster)(responseExecutionContext, log)

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
          app.AstraUploader.SPLUNK_SEARCH_PREFIX,
          successfulBatch.size,
          LBMMessageInfo.lbmMsgInfo(elements.head._1),
          LBMMessageInfo.lbmMsgInfo(elements.last._1)
        )
        // Move to next set of messages processing
        ........
      case Failure(e) =>
        if (failedToCommit == false) {
          failedToCommit = true
          log.warning(
            s"Failed to write ${currentBatch.length} messages received on " +
              s"topic=$topic to database, " +
              s"reason=${e.getMessage}. Action will be retried."
          )
        }


        context.system.scheduler.scheduleOnce(Duration(1, TimeUnit.SECONDS)) {
            //Schedule reprocessing same batch
        }
    }