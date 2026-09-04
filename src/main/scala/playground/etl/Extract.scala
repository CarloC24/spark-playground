package playground.etl

import org.apache.spark.sql.functions.{coalesce, col, concat, from_json, lit, trim, when}
import org.apache.spark.sql.{Column, Dataset, SparkSession}

/**
 * The E of ETL, and the only place in the pipeline that knows where rows come from.
 *
 * Every reader returns the same thing — a `Dataset[Person]` whose `hobbies` is always a
 * non-null array — so [[Transform]] and [[Load]] never learn where the rows came from.
 * That uniformity is the whole point: the JSON and Postgres sides have to work for it,
 * the Parquet side gets it for free, and downstream code can't tell the difference.
 */
object Extract {

  def apply(spark: SparkSession, config: PipelineConfig): Dataset[Person] =
    config.format match {
      case "json"     => fromJson(spark, config.input)
      case "parquet"  => fromParquet(spark, config.input)
      case "postgres" => fromPostgres(spark, config.input, config.jdbc)
      case other      => throw new IllegalArgumentException(s"unsupported format: $other")
    }

  /**
   * Forces raw hobbies JSON text into array-of-objects shape: pass an array through,
   * wrap a bare object, and treat null as empty rather than null so downstream code can
   * always assume a list.
   *
   * Shared by the JSON and Postgres readers. Both hand over a string column: the JSON
   * reader because [[Schemas.jsonReadSchema]] deliberately declares the field as text,
   * the Postgres reader because Spark's PostgresDialect maps `jsonb` to `StringType`.
   */
  def normalizeHobbies(rawJson: Column): Column = {
    val asArray = when(rawJson.isNull, lit("[]"))
      .when(trim(rawJson).startsWith("["), rawJson)
      .otherwise(concat(lit("["), rawJson, lit("]")))

    from_json(asArray, Schemas.hobbiesType)
  }

  /**
   * Reads the raw `hobbies`/`Hobbies` text (see [[Schemas.jsonReadSchema]] for why it is
   * text), takes whichever spelling the row used, and normalizes it.
   *
   * Requires `spark.sql.caseSensitive = true` for the life of the read — see
   * [[PeoplePipeline.session]].
   */
  def fromJson(spark: SparkSession, path: String): Dataset[Person] = {
    import spark.implicits._

    val raw = spark.read
      // The file is a pretty-printed JSON array, not one object per line.
      .option("multiLine", value = true)
      .schema(Schemas.jsonReadSchema)
      .json(path)

    raw
      .select(
        col("id"),
        col("name"),
        col("email"),
        col("age"),
        col("city"),
        normalizeHobbies(coalesce(col("hobbies"), col("Hobbies"))).as("hobbies")
      )
      .as[Person]
  }

  /**
   * Parquet needs no reshaping — it carries its own schema and `hobbies` is already a
   * proper array of structs. Partition discovery handles the `city=Lagos/` directories.
   *
   * The `select` is load-bearing, not tidiness. Partition columns are appended at the
   * *end* of the discovered schema regardless of where they sat when written, and
   * re-reading our own output drags the enrichment columns along too. Selecting by name
   * restores [[Person]]'s declared order and drops the extras.
   */
  def fromParquet(spark: SparkSession, path: String): Dataset[Person] = {
    import spark.implicits._
    spark.read.parquet(path).select(Schemas.personColumns: _*).as[Person]
  }

  /**
   * How many parallel JDBC connections a Postgres read opens. Absurd for 5 rows, but the
   * point is to exercise the partitioned read path: it is the single most common thing
   * people get wrong with Spark and JDBC, and with 5 rows the empty partitions are
   * visible in the UI rather than hidden.
   */
  val PostgresReadPartitions = 4

  /**
   * Reads a table through Spark's JDBC source and the PostgresDialect.
   *
   * Two things worth knowing about the plain `spark.read.jdbc` you'd write first:
   *
   *  - It runs on **one task**, whatever the cluster looks like. Spark opens one
   *    connection, issues one `SELECT`, and pulls every row through it. Parallelism only
   *    happens when you give it a numeric `partitionColumn` plus bounds; it then issues
   *    one range query per partition. The bounds are not discovered for you, hence the
   *    small extra query below.
   *  - With the default `fetchsize` of 0 the Postgres driver materializes the whole result
   *    set in memory before handing back the first row. Any positive value makes it
   *    stream with a server-side cursor.
   *
   * Type mapping is the dialect's: bigint → LongType, integer → IntegerType, text →
   * StringType, and jsonb → StringType, which is why `hobbies` goes through
   * [[normalizeHobbies]] exactly as the JSON reader's does. The value stored may be a
   * bare object rather than an array (see docker/postgres/init.sql), so the wrapping
   * matters here too.
   */
  def fromPostgres(spark: SparkSession, table: String, jdbc: JdbcConfig): Dataset[Person] = {
    import spark.implicits._

    val reader = spark.read
      .format("jdbc")
      .options(jdbc.options)
      .option("fetchsize", "1000")

    // One tiny query so the real read can be split into id ranges.
    val bounds = reader
      .option("dbtable", s"(SELECT min(id) AS lo, max(id) AS hi FROM $table) AS bounds")
      .load()
      .head()

    val raw =
      if (bounds.isNullAt(0)) {
        // Empty table: no bounds to split on, and Spark rejects null bounds anyway.
        reader.option("dbtable", table).load()
      } else {
        reader
          .option("dbtable", table)
          .option("partitionColumn", "id")
          .option("lowerBound", bounds.getLong(0))
          .option("upperBound", bounds.getLong(1))
          .option("numPartitions", PostgresReadPartitions)
          .load()
      }

    raw
      .select(
        col("id"),
        col("name"),
        col("email"),
        col("age"),
        col("city"),
        normalizeHobbies(col("hobbies")).as("hobbies")
      )
      .as[Person]
  }
}
