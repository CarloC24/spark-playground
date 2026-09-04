package playground.etl

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SaveMode}

/**
 * The L of ETL: Hive-style partitioned Parquet on disk, e.g.
 * `data/warehouse/people_enriched/city=Lagos/part-*.parquet`.
 *
 * The partition column's value lives in the directory name and is *not* stored in the
 * files, which is exactly what [[Extract.fromParquet]] relies on partition discovery to
 * put back.
 */
object Load {

  def apply(people: Dataset[PersonEnriched], config: PipelineConfig): Unit = {
    val partitionBy = config.partitionBy
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
      .parquet(config.output)
  }
}
