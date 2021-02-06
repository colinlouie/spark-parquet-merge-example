/** This example micro-application targets reading and writing Parquets.
  *
  * High-level (objective) requirement: Store metrics for the purposes of calculating moving-averages, with per-hour
  * granularity is all we care about.
  *
  * Low-level (tactical) requirement: Merge the output from a previous job, into the current job.
  */

/** This is the demaraction point from when we get raw input data into "actual" Spark processing.
  *
  * @param ts
  *   A normalized (truncated to hour granularity) epoch timestamp.
  * @param content
  *   The actual thing we care about.
  * @param count
  *   The number of occurrences of things we care about, according the wanted granularity (per hour).
  */
case class Record(
  ts: Int,
  content: String,
  count: BigInt = 1
) {

  /** Merges like-Records together.
    *
    * @param other
    *   instance of Record to merge with this Record.
    * @return
    *   new Record instance, as a result of merging this Record with the other Record.
    */
  def ++(other: Record): Record = {
    if (this.ts != other.ts) {
      throw new Exception(s"cannot merge. timestamps do not match (${this.ts} != ${other.ts}")
    }
    if (this.content != other.content) {
      throw new Exception(s"cannot merge. content does not match (${this.content} != ${other.content}")
    }
    Record(ts = this.ts, content = this.content, count = this.count + other.count)
  } // def ++

} // case class Record

object Main {

  /** Application entrypoint.
    *
    * @param args
    *   args/options as-is from the operating system.
    */
  def main(args: Array[String]): Unit = {
    val appName = "Parquet merge example"
    val sparkConfig = new org.apache.spark.SparkConf()
    val spark = org.apache.spark.sql.SparkSession.builder().config(sparkConfig).appName(appName).getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import org.apache.spark.sql.functions._

    // THE FOLLOWING CONTENT REPRESENTS THE "PREVIOUS" SPARK JOB. PLEASE READ IN ITS ENTIRETY UNTIL YOU REACH
    // THE "CURRENT" SPARK JOB.

    /** This is the input demarcation point. Assume information from streaming logs, or batch-reads from log files is
      * already performed, and parsed into the Record case class by the time we arrive here.
      *
      * If you only care about using Datasets, skip over r2 and r3. In fact, this example does not use the results from
      * r2/r3, only for illustration of an equivalent operation using RDDs. Choosing one over the other should be
      * considered as a trade-off between performance and readability.
      *
      * How to truncate to hour-granularity:
      * {{{
      *   scala> 1612559965 - (1612559965 % 3600)
      *   Int = 1612558800
      * }}}
      *
      * This is what a tabular representation looks like:
      * {{{
      *   scala> r1.toDF().show()
      *   +----------+-------+-----+
      *   |        ts|content|count|
      *   +----------+-------+-----+
      *   |1612558800|  hello|    1|
      *   |1612558800|  world|    1|
      *   |1612558800|  world|    1| <<<<< Goal: aggregate like-content (e.g. rows 2 & 3).
      *   +----------+-------+-----+
      * }}}
      */
    val r1 = sc.parallelize(
      Seq(
        Record(ts = 1612558800, content = "hello"),
        Record(ts = 1612558800, content = "world"),
        Record(ts = 1612558800, content = "world")
      )
    )

    /** Create a key-value tuple where the key is used to reduce (merge) like Records. */
    val r2 = r1.keyBy(x => x.ts.toString() + "-" + x.content)

    /** Merge like Records.
      *
      * After reducing r1, we achieved the goal of aggregating like-content.
      * {{{
      *   scala> r3.values.toDF().show()
      *   +----------+-------+-----+
      *   |        ts|content|count|
      *   +----------+-------+-----+
      *   |1612558800|  world|    2| <<<<< Goal from r1 achieved.
      *   |1612558800|  hello|    1|
      *   +----------+-------+-----+
      * }}}
      */
    val r3 = r2.reduceByKey(_ ++ _).values
    r3.toDF().show()

    /** Convert the RDD to a Dataset. */
    val previous = r1
      .toDF()
      .groupBy("ts", "content")
      .agg(
        sum("count").as("count") // aliases the column name "sum(count)" to just "count".
      )

    // Uncomment this next line if you want to write this dataset out to Parquet for testing. There are options to
    // allow clobbering any existing Parquets. I recommend against this since this wipes out historical data. I also
    // recommend making your storage append-only (from this application's RBAC/ACL) where applicable (e.g. AWS S3
    // buckets) to preserve auditability.
    //
    // previous.write.parquet("123.parquet")

    /** In Production, you would read the previous Parquet, which deserializes into a generic DataFrame. This is the
      * safest (not necessarily the best-choice) operation.
      *
      * {{{
      *   scala> val previous = spark.read.parquet("123.parquet")
      *   previous: org.apache.spark.sql.DataFrame = [ts: int, content: string ... 1 more field]
      * }}}
      *
      * You can directly read a Parquet as Dataset of Records if you choose. There are associated caveats that require
      * strict impedance-match. For instance, the `count` attribute originally chosen was Int, but the `sum()``
      * operation makes the new `count` column a BigInt (only makes sense since adding Ints may spill over to a BigInt).
      * Also beware that if any of the following changes: column-order, column-names, number of columns, this will fail.
      * If you need to deal with this, the first run of this breaking-change will likely need to be handled manually.
      * That is, you may need a RecordV2, RecordV3, etc... for migratory purposes, as well as an input parameter flag
      * for the versions.
      *
      * e.g. {{{spark-submit <JAR> --input-version=1 --output-version=2}}} for the breaking change, then proceed with
      * {{{spark-submit <JAR> --input-version=2 --output-version=2}}} for subsequent, automated runs.
      *
      * {{{
      *   scala> val previous = spark.read.parquet("123.parquet").as[Record]
      *   previous: org.apache.spark.sql.Dataset[Record]
      * }}}
      */

    // THIS IS WHERE THE "CURRENT" SPARK JOB RUN WOULD START.

    // Read the contents of the previous Spark job, where applicable.
    // val previous = spark.read.parquet("123.parquet").as[Record]

    val r4 = sc.parallelize(
      Seq(
        Record(ts = 1612558800, content = "hello"), // simulates a late-arrival log entry.
        Record(ts = 1612562400, content = "world"),
        Record(ts = 1612562400, content = "hello") // unrelated to the first entry, which belongs in a "previous" hour.
      )
    )

    /** Merge the previous data with the current, then run the aggregation. This is now ready to be queried for the last
      * x, y, z moving-averages (e.g. Show me the last hour, 24-hours, 7-days, 30-days, year, etc...).
      *
      * Assume "now" minus one hour is 1612562400, this is what the query would look like:
      * {{{
      *   scala> current.where($"ts" >= 1612562400).orderBy("ts", "count", "content").show()
      *   +----------+-------+-----+
      *   |        ts|content|count|
      *   +----------+-------+-----+
      *   |1612562400|  hello|    1|
      *   |1612562400|  world|    1|
      *   +----------+-------+-----+
      * }}}
      *
      * If you were to export/publish the dataset, this is all you would need (where "DropPoint" is your export
      * demarcation point):
      * {{{
      *   scala> DropPoint.export(current.where($"ts" >= 1612562400))
      * }}}
      *
      * Do NOT run `orderBy()` in Production queries. The following is only for illustration purposes.
      *
      * e.g.
      * {{{
      *   scala> current.orderBy("ts", "count", "content").show()
      *   +----------+-------+-----+
      *   |        ts|content|count|
      *   +----------+-------+-----+
      *   |1612558800|  hello|    2|
      *   |1612558800|  world|    2|
      *   |1612562400|  hello|    1|
      *   |1612562400|  world|    1|
      *   +----------+-------+-----+
      * }}}
      */
    val current = previous
      .union(r4.toDF())
      .groupBy("ts", "content")
      .agg(
        sum("count").as("count")
      )

    current.show()

    // Write the contents of the current Spark job, for a/the subsequent run.
    // current.write.parquet("456.parquet")

    spark.stop()
  } // def main

} // object Main
