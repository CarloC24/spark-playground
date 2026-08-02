package playground.etl

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

/**
 * One SparkSession for the whole test run.
 *
 * `SparkSession.builder().getOrCreate()` is already a per-JVM singleton, so suites cannot
 * each own one — and none of them may call `stop()`, since that would pull the session out
 * from under whichever suite runs next. The forked test JVM exits at the end of the run
 * and takes the session with it.
 */
object SparkTestSession {
  lazy val instance: SparkSession = {
    val spark = PeoplePipeline.session("etl-tests")
    spark.sparkContext.setLogLevel("WARN")
    // 200 shuffle partitions for 5 rows is all overhead.
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    spark
  }
}

trait SparkSessionTestBase extends AnyFunSuite {
  // A val, not a def: `import spark.implicits._` needs a stable identifier.
  protected val spark: SparkSession = SparkTestSession.instance

  protected val peopleJson = "src/main/resources/people.json"
}
