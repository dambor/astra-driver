ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "AstraDriverProject",
    libraryDependencies ++= Seq(
      // Cassandra DataStax Driver
      "com.datastax.oss" % "java-driver-core" % "4.13.0",
      "com.datastax.oss" % "java-driver-query-builder" % "4.17.0",
      
      // Akka for actor system and logging
      "com.typesafe.akka" %% "akka-actor" % "2.6.20",
      "com.typesafe.akka" %% "akka-stream" % "2.6.20",
      "com.typesafe.akka" %% "akka-testkit" % "2.6.20" % Test,
      
      // SLF4J logging (fixes the ClassNotFoundException)
      "com.typesafe.akka" %% "akka-slf4j" % "2.6.20",
      "ch.qos.logback" % "logback-classic" % "1.2.12",
      
      // Scala Java8 compatibility for Future conversions
      "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.2",
      
      // Optional: Snappy compression (uncomment if needed)
      // "org.xerial.snappy" % "snappy-java" % "1.1.9.1",
      
      // Configuration
      "com.typesafe" % "config" % "1.4.2",
      
      // Testing
      "org.scalatest" %% "scalatest" % "3.2.17" % Test,
      "org.mockito" %% "mockito-scala" % "1.17.12" % Test
    ),
    
    // Compiler options
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-unchecked"
    )
  )