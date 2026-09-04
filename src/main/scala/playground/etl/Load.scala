package playground.etl

import org.apache.spark.sql.functions.{col, to_json}
import org.apache.spark.sql.{Dataset, SaveMode}

/**
 * The L of ETL. Two sinks:
 *
 *  - Hive-style partitioned Parquet on disk, e.g.
 *    `data/warehouse/people_enriched/city=Lagos/part-*.parquet`. The partition column's
 *    value lives in the directory name and is *not* stored in the files, which is exactly
 *    what [[Extract.fromParquet]] relies on partition discovery to put back.
 *  - A Postgres table, through Spark's JDBC writer.
 */
object Load {

  def apply(people: Dataset[PersonEnriched], config: PipelineConfig): Unit =
    config.outputFormat match {
      case "parquet"  => toParquet(people, config.output, config.partitionBy)
      case "postgres" => toPostgres(people, config.output, config.jdbc)
      case other      => throw new IllegalArgumentException(s"unsupported output format: $other")
    }

  def toParquet(people: Dataset[PersonEnriched], output: String, partitionBy: String): Unit = {
    if (!people.columns.contains(partitionBy)) {
      throw new IllegalArgumentException(
        s"--partition-by $partitionBy is not a column of the output: " +
          people.columns.mkString(", ")
      )
    }

    people
      // Without this, every one of local[*]'s tasks writes its own file into every
      // partition directory it happens to hold a row for. Repartitioning first collects
      // each partition value onto one task, so each directory gets a single file.
      .repartition(col(partitionBy))
      .write
      // Overwrite replaces the whole output directory, which makes reruns idempotent.
      // Per-partition replacement would want spark.sql.sources.partitionOverwriteMode=dynamic.
      .mode(SaveMode.Overwrite)
      .partitionBy(partitionBy)
      .parquet(output)
  }

  /**
   * How many parallel JDBC connections a write opens. Each DataFrame partition becomes one
   * connection running batched `INSERT`s; this caps it so a wide upstream shuffle doesn't
   * open dozens against a local Postgres.
   */
  val PostgresWritePartitions = 4

  /**
   * Writes the enriched rows into a pre-existing table (see docker/postgres/init.sql).
   *
   * Three adjustments before the write, each forced by the JDBC path:
   *
   *  - **`hobbies` becomes JSON text.** The dialect can write arrays of primitives but not
   *    an array of structs, so the column goes over the wire as a string. It still lands
   *    in a `jsonb` column because the URL carries `stringtype=unspecified`
   *    (see [[JdbcConfig]]).
   *  - **Column names become snake_case.** Spark double-quotes identifiers in the SQL it
   *    generates, so an `ageGroup` column would create or match only a case-sensitive
   *    `"ageGroup"` column that every later query has to quote. Renaming here keeps the
   *    table idiomatic. With `spark.sql.caseSensitive=true` on this session the insert
   *    statement is also built by exact name, so a mismatch fails loudly instead of
   *    inserting into the wrong column.
   *  - **`truncate=true`.** Overwrite mode drops and recreates the table by default,
   *    which would lose the `jsonb` type and the primary key. Truncating keeps the DDL
   *    and just empties the rows, and reruns stay idempotent.
   */
  def toPostgres(people: Dataset[PersonEnriched], table: String, jdbc: JdbcConfig): Unit =
    people
      .withColumn("hobbies", to_json(col("hobbies")))
      .withColumnRenamed("ageGroup", "age_group")
      .withColumnRenamed("emailDomain", "email_domain")
      .withColumnRenamed("hobbyCount", "hobby_count")
      .write
      .format("jdbc")
      .options(jdbc.options)
      .option("dbtable", table)
      .option("truncate", "true")
      .option("numPartitions", PostgresWritePartitions)
      // Rows per INSERT batch. The default is already 1000; stated so it is visible.
      .option("batchsize", "1000")
      .mode(SaveMode.Overwrite)
      .save()
}
