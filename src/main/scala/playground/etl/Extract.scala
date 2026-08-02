package playground.etl

import org.apache.spark.sql.functions.{coalesce, col, concat, from_json, lit, trim, when}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * The E of ETL, and the only place in the pipeline that knows what a file format is.
 *
 * Both readers return the same thing — a `Dataset[Person]` whose `hobbies` is always a
 * non-null array — so [[Transform]] and [[Load]] never learn where the rows came from.
 * That uniformity is the whole point: the JSON side has to work for it, the Parquet side
 * gets it for free, and downstream code can't tell the difference.
 */
object Extract {

  def apply(spark: SparkSession, config: PipelineConfig): Dataset[Person] =
    config.format match {
      case "json"    => fromJson(spark, config.input)
      case "parquet" => fromParquet(spark, config.input)
      case other     => throw new IllegalArgumentException(s"unsupported format: $other")
    }

  /**
   * Reads the raw `hobbies`/`Hobbies` text (see [[Schemas.jsonReadSchema]] for why it is
   * text) and forces it into array-of-objects shape: pass an array through, wrap a bare
   * object, and treat a missing key as empty rather than null so downstream code can
   * always assume a list.
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

    // Whichever spelling this row used, as raw JSON text.
    val rawHobbies = coalesce(col("hobbies"), col("Hobbies"))

    val hobbiesJson = when(rawHobbies.isNull, lit("[]"))
      .when(trim(rawHobbies).startsWith("["), rawHobbies)
      .otherwise(concat(lit("["), rawHobbies, lit("]")))

    raw
      .select(
        col("id"),
        col("name"),
        col("email"),
        col("age"),
        col("city"),
        from_json(hobbiesJson, Schemas.hobbiesType).as("hobbies")
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
}
