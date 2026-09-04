package playground.etl

import java.sql.DriverManager

/**
 * The Postgres counterpart of [[PipelineRoundTripSpec]]'s read-back check: JSON and the
 * `people` table must extract to the same `Dataset[Person]`.
 *
 * Needs the Compose Postgres (`docker compose up -d`). When it is not reachable every
 * test here is *canceled* rather than failed, so `sbt test` stays green without Docker.
 * Point `PG_JDBC_URL` / `PGUSER` / `PGPASSWORD` elsewhere to run against another server.
 */
class PostgresExtractSpec extends SparkSessionTestBase {

  private val jdbc: JdbcConfig =
    JdbcConfig.default.copy(url = sys.env.getOrElse("PG_JDBC_URL", JdbcConfig.DefaultUrl))

  private lazy val reachable: Boolean =
    try {
      DriverManager
        .getConnection(jdbc.url + "?connectTimeout=2&loginTimeout=2", jdbc.user, jdbc.password)
        .close()
      true
    } catch { case _: java.sql.SQLException => false }

  /** A test that is canceled, not failed, without a database to talk to. */
  private def pgTest(name: String)(body: => Unit): Unit =
    test(name) {
      assume(reachable, s"Postgres not reachable at ${jdbc.url}; start it with `docker compose up -d`")
      body
    }

  private lazy val fromJson: Seq[Person] =
    Extract.fromJson(spark, peopleJson).collect().sortBy(_.id).toSeq

  private lazy val fromTable: Seq[Person] =
    Extract.fromPostgres(spark, "people", jdbc).collect().sortBy(_.id).toSeq

  pgTest("the people table extracts to the same records the JSON does") {
    assert(fromTable == fromJson)
  }

  pgTest("a bare-object jsonb value comes back as a one-element array") {
    // id 2 is stored as {"hobbieName": "wrestling"}, not [{...}] — see init.sql.
    assert(fromTable(1).hobbies == Seq(Hobby("wrestling")))
  }

  pgTest("the read is split across id ranges") {
    val partitions = Extract.fromPostgres(spark, "people", jdbc).rdd.getNumPartitions
    assert(partitions == Extract.PostgresReadPartitions)
  }

  pgTest("a config with --format postgres drives Extract to the table") {
    val config = PipelineConfig.parse(Array("--format", "postgres", "--jdbc-url", jdbc.url))
    assert(Extract(spark, config).collect().sortBy(_.id).toSeq == fromJson)
  }
}
