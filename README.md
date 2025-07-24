# AstraDB Driver with Chaos Engineering Tests

This repository contains the AstraDB driver implementation along with comprehensive chaos engineering test scenarios designed to simulate and investigate missing records issues in production environments.


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

