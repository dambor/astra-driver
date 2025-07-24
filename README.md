# AstraDB Driver with Chaos Engineering Tests

This repository contains the AstraDB driver implementation along with comprehensive chaos engineering test scenarios designed to simulate and investigate missing records issues in production environments.

## Overview

This project provides:
- **Production-ready AstraDB driver** with optimized configuration and retry logic
- **Chaos engineering framework** for testing driver reliability under adverse conditions
- **Missing records simulation** to reproduce and analyze silent data loss scenarios
- **Queue overflow testing** to validate driver behavior under high load
- **Comprehensive test suite** for validating dual-write architectures


## Features

### 🚀 **Production-Ready AstraDB Driver**
- **Optimized connection pooling** with configurable pool sizes and timeouts
- **Advanced request throttling** to prevent queue overflow
- **Comprehensive retry logic** with exponential backoff
- **Session management** with automatic recovery and connection pooling
- **Metrics and monitoring** integration for production observability
- **Dual-write support** for hybrid cloud/on-premises architectures

### 🐒 **Chaos Engineering Framework**
- **ChaosWrapper** for injecting failures, latency, and errors during testing
- **Configurable chaos modes** (failure injection, latency simulation, timeout testing)
- **Statistical tracking** of chaos injection rates and system behavior
- **Production-like load simulation** with concurrent writers and realistic batch sizes
- **Queue overflow simulation** to test driver behavior under extreme load

### 🔍 **Missing Records Investigation**
- **Silent data loss detection** scenarios that reproduce production issues
- **Queue overflow testing** to identify when records are silently dropped
- **Application logging vs database reality** comparison testing
- **Dual-write validation** to ensure consistency between multiple destinations
- **Volume tracking** to detect discrepancies between expected and actual record counts

## Quick Start

### Prerequisites
- Scala 2.13+
- SBT 1.11+
- AstraDB database with application token
- Java 11+

### Setup

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd astra-driver
   ```

2. **Configure your AstraDB connection**:
   ```bash
   # Set your application token
   export ASTRA_DB_APPLICATION_TOKEN=your_token_here
   
   # Update application.conf with your settings
   cp Config.template .env
   # Edit .env with your actual values
   ```

3. **Update configuration**:
   ```hocon
   # src/main/resources/application.conf
   astra {
     connection {
       secure-bundle-path = "secure-connect-your-db.zip"
     }
     driver {
       local-datacenter = "us-east-1"
       max-queue-size = 25000
       max-requests-per-connection = 2048
     }
   }
   ```

### Running Tests

#### Basic Driver Test
```bash
sbt "runMain examples.SimpleDemoApp"
```

#### Missing Records Simulation
```bash
# Run the comprehensive missing records test
sbt -J-Xmx2g "runMain examples.QueueOverflowMissingRecordsTest"
```

#### Chaos Engineering Tests
```bash
# Basic chaos test
sbt "runMain examples.ChaosTest"

# Latency injection test
sbt "runMain examples.LatencyChaosTest"

# Comprehensive test suite
sbt "runMain examples.ComprehensiveTestSuite"
```

#### Silent Data Loss Simulation
```bash
# Test partial silent data loss scenarios
sbt "runMain examples.PartialSilentDataLossTest"

# Test missing records with different session types
sbt "runMain examples.MissingRecordsSimulationTest"
```

## Key Test Scenarios

### 1. Queue Overflow Missing Records Test
**Purpose**: Simulate production scenarios where high-frequency writes overwhelm the driver's request queue, causing silent record drops.

**What it tests**:
- Driver behavior when `max-queue-size` is exceeded
- Silent data loss without error indication
- Application logging vs actual database state discrepancies
- Queue utilization monitoring and overflow detection

**Expected outcome**: Reproduces the exact production issue where applications log "100% success" but records are missing from the database.

### 2. Dual-Write Validation Tests
**Purpose**: Test consistency between multiple database destinations (e.g., AstraDB + local Cassandra).

**What it tests**:
- Concurrent writes to multiple destinations
- Failure isolation (one destination fails, other succeeds)
- Volume reconciliation between destinations
- Silent failures in one destination while other succeeds

### 3. Chaos Engineering Tests
**Purpose**: Validate driver resilience under adverse conditions.

**What it tests**:
- Network latency injection
- Connection failure simulation
- Timeout and retry behavior
- Error recovery mechanisms
- System behavior under degraded conditions

## Configuration Options

### Driver Configuration
```hocon
astra {
  driver {
    # Queue management
    max-queue-size = 25000                    # Prevent overflow (default: 10000)
    max-requests-per-connection = 2048        # Connection capacity
    max-local-connections-per-host = 12       # Pool size
    
    # Timeouts
    request-timeout-ms = 15000                # Request timeout
    
    # Throttling
    throttler {
      max-concurrent-requests = 15000         # Concurrent request limit
    }
  }
}
```

### Chaos Testing Configuration
```scala
// Enable chaos mode for testing
chaosWrapper.enableChaos()
chaosWrapper.setChaosLevel(0.3)              // 30% failure rate
chaosWrapper.setChaosMode(Failure)           // Failure injection mode

// Or latency injection
chaosWrapper.setChaosMode(Latency, Some(100.milliseconds))
```


## Troubleshooting

### Common Issues

#### OutOfMemoryError During Tests
```bash
# Increase heap size for large tests
sbt -J-Xmx2g "runMain examples.QueueOverflowMissingRecordsTest"
```

#### Connection Issues
```bash
# Verify environment variables
echo $ASTRA_DB_APPLICATION_TOKEN

# Check secure bundle path in application.conf
```

#### Test Timeouts
```bash
# Some tests may take time due to retry logic
# Monitor logs for progress indicators
```

### Test Configuration
```scala
// Reduce test scale for faster execution
val RECORDS_PER_BATCH = 100      // Reduce from 500
val TOTAL_BATCHES = 5            // Reduce from 10
```

