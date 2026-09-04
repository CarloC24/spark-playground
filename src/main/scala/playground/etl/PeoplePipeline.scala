package playground.etl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_json}

/**
 * Reads people from JSON, from a Hive-partitioned Parquet directory, or from a Postgres
 * table; derives a few columns; and writes partitioned Parquet or a Postgres table.
 *
 * {{{
 * sbt run
 * sbt "run --input data/warehouse/people_enriched --output data/warehouse/people_reprocessed"
 * sbt "run --format postgres --output-format postgres"      # needs: docker compose up -d
 * }}}
 *
 * The second form reads the first form's output, which is the point: every input path
 * converges on the same `Dataset[Person]` before [[Transform]] ever sees them, and every
 * output path starts from the same `Dataset[PersonEnriched]`.
 */
object PeoplePipeline {

  /**
   * `spark.sql.caseSensitive` is set here on the builder rather than with a later
   * `spark.conf.set`, and it has to stay on for the life of the read.
   *
   * It is what lets `hobbies` and `Hobbies` coexist as distinct columns in
   * [[Schemas.jsonReadSchema]]; under the default (false) they collide as duplicate
   * names. And it cannot be flipped back once the Dataset is built: FileSourceStrategy
   * re-resolves the required columns against the file schema during *physical planning*,
   * which happens lazily at action time, so turning it off early gets you
   * AMBIGUOUS_REFERENCE on the first count()/write(), long after the plan looked fine.
   */
  def session(appName: String): SparkSession =
    SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.caseSensitive", "true")
      .getOrCreate()

  def main(args: Array[String]): Unit = {
    val config = PipelineConfig.parse(args)

    val spark = session("people-pipeline")
    spark.sparkContext.setLogLevel("WARN")

    try {
      val people = Extract(spark, config)
      println(s"Extracted ${people.count()} person records from ${config.input} (${config.format})")

      val enriched = Transform(spark, people)
      enriched.printSchema()

      // show() renders a struct as {basketball}: positional values with the field names
      // stripped. to_json prints the column as the JSON it actually is, so the objects
      // come out as {"hobbieName":"basketball"} rather than a bare value.
      enriched.withColumn("hobbies", to_json(col("hobbies"))).show(truncate = false)

      Load(enriched, config)
      config.outputFormat match {
        case "postgres" => println(s"Wrote ${enriched.count()} rows to Postgres table ${config.output}")
        case _          => println(s"Wrote Parquet to ${config.output}, partitioned by ${config.partitionBy}")
      }
    } finally {
      spark.stop()
    }
  }
}
