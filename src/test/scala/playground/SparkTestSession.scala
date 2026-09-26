package playground

import org.apache.spark.sql.SparkSession

/**
 * One SparkSession for the whole test JVM. Suites mix this in.
 *
 * Nobody calls `stop()`. The obvious `afterAll { spark.stop() }` is a trap: the first suite
 * to finish would stop the session every later suite is holding, and those fail with
 * "SparkContext has been shutdown" depending on suite order. The session dies with the
 * forked test JVM, which is enough.
 */
trait SparkTestSession {
  lazy val spark: SparkSession = SparkTestSession.instance
}

object SparkTestSession {

  /**
   * `local[2]` rather than `local[*]`: enough parallelism to catch anything genuinely
   * order-dependent, few enough slots to keep failures reproducible.
   *
   * The shuffle partition default of 200 plans 200 tasks over five rows, and the UI would
   * bind port 4040 and collide with an `sbt run` left going in another terminal.
   */
  lazy val instance: SparkSession = {
    val session = SparkSession
      .builder()
      .appName("tests")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    session
  }
}
