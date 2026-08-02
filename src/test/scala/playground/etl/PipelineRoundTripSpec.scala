package playground.etl

import java.io.File
import java.nio.file.Files

/**
 * The test that justifies the whole [[Extract]] design: run JSON all the way through to
 * Parquet, read that Parquet back, and assert the two paths produce the same
 * `Dataset[Person]`. If normalization ever leaks out of the readers and into
 * [[Transform]], this is what notices.
 */
class PipelineRoundTripSpec extends SparkSessionTestBase {

  private lazy val outputDir: String =
    Files.createTempDirectory("people-etl-").resolve("people_enriched").toString

  private lazy val config =
    PipelineConfig(peopleJson, "json", outputDir, PipelineConfig.DefaultPartitionBy)

  /** Extracted from JSON, then written out as partitioned Parquet. Runs once. */
  private lazy val fromJson: Seq[Person] = {
    val people = Extract(spark, config)
    Load(Transform(spark, people), config)
    people.collect().sortBy(_.id).toSeq
  }

  test("the output is laid out as Hive partitions") {
    fromJson // force the write
    val partitions =
      new File(outputDir).listFiles().filter(_.isDirectory).map(_.getName).sorted.toSeq

    assert(
      partitions == Seq(
        "city=Bengaluru",
        "city=Lagos",
        "city=Milan",
        "city=Oslo",
        "city=Shanghai"
      )
    )
  }

  test("each partition directory holds exactly one Parquet file") {
    fromJson
    val perPartition = new File(outputDir)
      .listFiles()
      .filter(_.isDirectory)
      .map(_.listFiles().count(_.getName.endsWith(".parquet")))

    assert(perPartition.forall(_ == 1), s"got ${perPartition.mkString(", ")} files per partition")
  }

  test("reading the Parquet back gives the same records the JSON did") {
    val expected = fromJson
    val actual   = Extract.fromParquet(spark, outputDir).collect().sortBy(_.id).toSeq

    assert(actual == expected)
  }

  test("city survives the round trip even though it lives only in the directory name") {
    fromJson
    val cities = Extract
      .fromParquet(spark, outputDir)
      .collect()
      .sortBy(_.id)
      .map(_.city)
      .toSeq

    assert(cities == Seq("Lagos", "Oslo", "Shanghai", "Bengaluru", "Milan"))
  }

  test("a config pointing at the output infers the parquet format") {
    val reread = PipelineConfig.parse(Array("--input", outputDir))
    assert(reread.format == "parquet")
  }
}
