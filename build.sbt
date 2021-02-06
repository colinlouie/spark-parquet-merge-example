name := "spark-parquet-merge-example"

version := "0.1"

scalaVersion := "2.12.10"

lazy val sparkVersion = "3.0.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

/** --------------------------------------- */
/** Required for operation of Apache Spark. */
/** --------------------------------------- */

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Provided

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Provided

/** ------------------------- */
/** Required for application. */
/** ------------------------- */

// None for now.

/** --------------------- */
/** Required for testing. */
/** --------------------- */

// Forking the JVM is required to pick up the custom configuration.
fork in Test := true
