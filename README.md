# Astra Driver Project

A Scala project for connecting to DataStax Astra DB (Cassandra-as-a-Service) with comprehensive configuration, retry logic, and batch processing capabilities.

## Features

- **Astra DB Integration**: Full configuration for DataStax Astra with secure cloud connectivity
- **Connection Management**: Singleton session management with automatic retry
- **Async Query Execution**: Non-blocking query execution with Future-based results  
- **Batch Processing**: Queue-based batch processing with retry logic
- **Comprehensive Monitoring**: Built-in metrics and request logging
- **Actor System Integration**: Akka-based logging and scheduling

## Project Structure

```
src/
├── main/
│   ├── scala/
│   │   ├── astra/
│   │   │   ├── SessionConfig.scala      # Driver configuration
│   │   │   ├── Config.scala             # Application configuration
│   │   │   ├── AstraSession.scala       # Session factory
│   │   │   ├── QueryExecutor.scala      # Async query execution
│   │   │   ├── BaseCluster.scala        # Session management
│   │   │   └── AstraUploader.scala      # Batch processing with retry
│   │   └── Main.scala                   # Application entry point
│   └── resources/
│       └── application.conf             # Configuration file
└── test/
    └── scala/
        └── QueryExecutorSpec.scala      # Unit tests
```

## Configuration

### Environment Variables
Set these environment variables for authentication:
```bash
export ASTRA_CLIENT_ID="your-client-id"
export ASTRA_CLIENT_SECRET="your-client-secret"
```

### Application Configuration
Update `src/main/resources/application.conf`:
```hocon
astra {
  connection {
    secure-bundle-path = "path/to/your/secure-connect-database.zip"
  }
  driver {
    local-datacenter = "your-datacenter"  # e.g. "us-east-1"
  }
}
```

## Usage

### Running the Application
```bash
sbt run
```

### Running Tests
```bash
sbt test
```

### Creating a Session
```scala
import astra.AstraSession

// With keyspace
val session = AstraSession.createSession("your_keyspace")

// Without keyspace
val session = AstraSession.createSession()
```

### Executing Queries
```scala
import astra.QueryExecutor
import scala.collection.mutable.Queue

// Prepare your statements
val statements = Queue((boundStatement, session))

// Execute asynchronously
val futureResults = QueryExecutor.execute(statements)(cluster)

futureResults.onComplete {
  case Success(results) => println(s"Executed ${results.size} statements")
  case Failure(exception) => println(s"Failed: ${exception.getMessage}")
}
```

### Batch Processing with Retry
```scala
import astra.AstraUploader

val uploader = new AstraUploader()
uploader.processBatch(elements, topic, statements)
```

## Key Features Explained

### Driver Configuration
- **Connection Pooling**: Optimized connection pool settings
- **Load Balancing**: DataStax default load balancing policy
- **Metrics**: Comprehensive metrics collection for monitoring
- **Request Logging**: Detailed logging for slow, error, and warning conditions
- **Timeouts**: Configurable request timeouts

### Session Management
- **Singleton Pattern**: Single session instance with thread-safe creation
- **Auto-Recovery**: Automatic session reset on connection issues
- **Resource Management**: Proper cleanup and connection closing

### Query Execution
- **Async Processing**: Non-blocking query execution using CompletionStage to Future conversion
- **Error Handling**: Comprehensive exception handling with logging
- **Batch Support**: Queue-based batch processing for high throughput

### Retry Logic
- **Exponential Backoff**: Built-in retry mechanism with scheduling
- **Failure Detection**: Automatic detection of failed commits
- **Monitoring**: Detailed logging of retry attempts and success/failure states

## Dependencies

- **DataStax Java Driver**: Core Cassandra connectivity
- **Akka**: Actor system for logging and scheduling  
- **Scala Java8 Compat**: Future/CompletionStage conversion
- **Typesafe Config**: Configuration management
- **ScalaTest**: Testing framework
- **Mockito**: Mocking for unit tests

## Getting Started

1. **Clone and build**:
   ```bash
   sbt compile
   ```

2. **Download your Astra secure bundle** from the Astra console

3. **Set environment variables** for authentication

4. **Update configuration** with your datacenter and bundle path

5. **Run the application**:
   ```bash
   sbt run
   ```

## Optional: Compression

To enable Snappy compression, uncomment the line in `SessionConfig.scala` and add the dependency:

```scala
libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.9.1"
```

## Monitoring

The driver includes comprehensive metrics for:
- **Session metrics**: CQL requests, client timeouts
- **Node metrics**: Connection pools, in-flight requests, timeouts, retries
- **Request logging**: Success, slow, and error request logging

Monitor these through your application logs and metrics collection system.