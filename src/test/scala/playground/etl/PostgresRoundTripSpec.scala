package playground.etl

import java.sql.{Connection, DriverManager}

import org.scalatest.BeforeAndAfterAll

/**
 * The Postgres counterpart of [[PipelineRoundTripSpec]]: JSON and the `people` table
 * must extract to the same `Dataset[Person]`, and what [[Load.toPostgres]] writes must
 * come back through [[Extract.fromPostgres]] unchanged.
 *
 * Needs the Compose Postgres (`docker compose up -d`). When it is not reachable every
 * test here is *canceled* rather than failed, so `sbt test` stays green without Docker.
 * Point `PG_JDBC_URL` / `PGUSER` / `PGPASSWORD` elsewhere to run against another server.
 *
 * Writes go to a scratch table with the same DDL as `people_enriched`, created here and
 * dropped afterwards, so the real sink table is never touched by the test run.
 */
class PostgresRoundTripSpec extends SparkSessionTestBase with BeforeAndAfterAll {

  private val jdbc: JdbcConfig =
    JdbcConfig.default.copy(url = sys.env.getOrElse("PG_JDBC_URL", JdbcConfig.DefaultUrl))

  private val sinkTable = "people_enriched_test"

  private def connect(): Connection =
    DriverManager.getConnection(jdbc.url + "&connectTimeout=2&loginTimeout=2", jdbc.user, jdbc.password)

  private lazy val reachable: Boolean =
    try { connect().close(); true }
    catch { case _: java.sql.SQLException => false }

  /** A test that is canceled, not failed, without a database to talk to. */
  private def pgTest(name: String)(body: => Unit): Unit =
    test(name) {
      assume(reachable, s"Postgres not reachable at ${jdbc.url}; start it with `docker compose up -d`")
      body
    }

  private def sql(statement: String): Unit = {
    val c = connect()
    try c.createStatement().execute(statement)
    finally c.close()
  }

  private def queryLongs(query: String): Seq[Long] = {
    val c = connect()
    try {
      val rs  = c.createStatement().executeQuery(query)
      val buf = Seq.newBuilder[Long]
      while (rs.next()) buf += rs.getLong(1)
      buf.result()
    } finally c.close()
  }

  private def queryStrings(query: String): Seq[String] = {
    val c = connect()
    try {
      val rs  = c.createStatement().executeQuery(query)
      val buf = Seq.newBuilder[String]
      while (rs.next()) buf += rs.getString(1)
      buf.result()
    } finally c.close()
  }

  override protected def beforeAll(): Unit =
    if (reachable) {
      sql(s"DROP TABLE IF EXISTS $sinkTable")
      sql(
        s"""CREATE TABLE $sinkTable (
           |  id bigint PRIMARY KEY, name text, email text, age integer, city text,
           |  hobbies jsonb, age_group text, email_domain text, hobby_count integer
           |)""".stripMargin
      )
    }

  override protected def afterAll(): Unit =
    if (reachable) sql(s"DROP TABLE IF EXISTS $sinkTable")

  private lazy val fromJson: Seq[Person] =
    Extract.fromJson(spark, peopleJson).collect().sortBy(_.id).toSeq

  private lazy val fromTable: Seq[Person] =
    Extract.fromPostgres(spark, "people", jdbc).collect().sortBy(_.id).toSeq

  /** Enriches the JSON records and writes them to the scratch table. Runs once. */
  private lazy val written: Seq[Person] = {
    val people = Extract.fromJson(spark, peopleJson)
    Load.toPostgres(Transform(spark, people), sinkTable, jdbc)
    fromJson
  }

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

  pgTest("every enriched row lands, with hobbies stored as a jsonb array") {
    written
    assert(queryLongs(s"SELECT count(*) FROM $sinkTable") == Seq(5))
    assert(queryStrings(s"SELECT DISTINCT jsonb_typeof(hobbies) FROM $sinkTable") == Seq("array"))
  }

  pgTest("the derived columns arrive under their snake_case names") {
    written
    assert(queryStrings(s"SELECT age_group FROM $sinkTable WHERE id = 1") == Seq("under-35"))
    assert(queryStrings(s"SELECT email_domain FROM $sinkTable WHERE id = 1") == Seq("example.com"))
    assert(queryLongs(s"SELECT hobby_count FROM $sinkTable WHERE id = 1") == Seq(2))
  }

  pgTest("rerunning the load truncates rather than duplicating or dropping the table") {
    written
    Load.toPostgres(Transform(spark, Extract.fromJson(spark, peopleJson)), sinkTable, jdbc)
    assert(queryLongs(s"SELECT count(*) FROM $sinkTable") == Seq(5))
    // The table kept its DDL: a Spark-created replacement would have typed hobbies as text.
    assert(
      queryStrings(
        s"SELECT data_type FROM information_schema.columns WHERE table_name = '$sinkTable' AND column_name = 'hobbies'"
      ) == Seq("jsonb")
    )
  }

  pgTest("reading the sink back as Person gives the records the JSON did") {
    val expected = written
    val actual   = Extract.fromPostgres(spark, sinkTable, jdbc).collect().sortBy(_.id).toSeq
    assert(actual == expected)
  }
}
