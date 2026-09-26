package playground

import java.sql.DriverManager

import org.apache.spark.sql.types.StringType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkPostgresSpec extends AnyFunSuite with Matchers with SparkTestSession {

  /**
   * Tests needing the container cancel rather than fail when it is down, so `sbt test` is
   * not red on a machine with Docker stopped. `lazy` so a suite of these opens one probe
   * connection rather than one per test.
   */
  private lazy val postgresIsUp: Boolean =
    try {
      val c = DriverManager.getConnection(SparkPostgres.JdbcUrl, SparkPostgres.User, SparkPostgres.Password)
      c.close()
      true
    } catch { case _: Throwable => false }

  private val NotRunning = "Postgres is not running; `docker compose up -d`"

  test("the session builds and runs a job") {
    import spark.implicits._
    Seq(1, 2, 3).toDF("n").count() shouldBe 3
  }

  test("reads every row of the people table") {
    assume(postgresIsUp, NotRunning)
    SparkPostgres.read(spark, "people").count() shouldBe 5
  }

  test("jsonb arrives as text, not as a struct") {
    assume(postgresIsUp, NotRunning)
    // PostgresDialect has no richer mapping, which is why anything reading `hobbies` has
    // to parse it. Pinning it here so the day it changes, a test says so.
    SparkPostgres.read(spark, "people").schema("hobbies").dataType shouldBe StringType
  }

  test("an aliased subquery works as dbtable") {
    assume(postgresIsUp, NotRunning)
    val byCity = SparkPostgres.read(
      spark,
      "(SELECT city, count(*) AS n FROM people GROUP BY city) AS by_city"
    )
    // A set, not a sequence: Spark promises no ordering without an orderBy.
    byCity.collect().map(_.getAs[String]("city")).toSet shouldBe
      Set("Lagos", "Oslo", "Shanghai", "Bengaluru", "Milan")
  }
}
