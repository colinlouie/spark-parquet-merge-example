# Parquet merge example

This example micro-application demonstrates the bare minimum usage of RDDs and Datasets to merge the output of a/the
Spark job, with the current Spark job, to produced a single output, for you guessed it, a subsequent Spark job.

One method of getting the "previous" data into this job is covered in the `scopt-example`, and orchestration can be
managed in a multitude of methods, including, but not limited to: Apache Airflow, use a roll-your-own RDBMS-based
solution, enumerate a directory for the latest Parquet, etc.

This approach assumes the following:
- You do not feed in the same log data more than once.
- Processing out-of-order is considered normal and does not impact the outcome.
- Only a single instance of this Spark job runs at any given time.

This is a part of a series of micro-applications that integrate into a larger application that utilizes Apache Spark.

This is the target execution:

`spark-submit target/scala-2.12/spark-parquet-merge-example-assembly-0.1.jar`

This has been tested with Spark 3.0.0 / Scala 2.12.10.

# Nomenclature

All occurrences of `Spark` refer to Apache Spark. DataFrames and Datasets are fungible for the most part. Please see the
Apache Spark website, as well as comments in `Main.scala` for the nuances.
