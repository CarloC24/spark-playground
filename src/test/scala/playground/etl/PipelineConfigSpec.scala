package playground.etl

import org.scalatest.funsuite.AnyFunSuite

/** Pure argument parsing; no SparkSession involved. */
class PipelineConfigSpec extends AnyFunSuite {

  test("no arguments means the JSON file to partitioned Parquet") {
    val c = PipelineConfig.parse(Array.empty)
    assert(c.input == PipelineConfig.DefaultInput)
    assert(c.format == "json")
    assert(c.output == PipelineConfig.DefaultOutput)
    assert(c.outputFormat == "parquet")
    assert(c.partitionBy == "city")
  }

  test("a bare positional argument is the input, and a .json path infers json") {
    val c = PipelineConfig.parse(Array("other.json"))
    assert(c.input == "other.json")
    assert(c.format == "json")
  }

  test("a non-.json input infers parquet") {
    assert(PipelineConfig.parse(Array("--input", "some/dir")).format == "parquet")
  }

  test("--format postgres defaults the input to the people table") {
    val c = PipelineConfig.parse(Array("--format", "postgres"))
    assert(c.format == "postgres")
    assert(c.input == "people")
  }

  test("--format postgres with an explicit --input keeps the table name given") {
    val c = PipelineConfig.parse(Array("--format", "postgres", "--input", "people_enriched"))
    assert(c.input == "people_enriched")
  }

  test("--output-format postgres defaults the output to the people_enriched table") {
    val c = PipelineConfig.parse(Array("--output-format", "postgres"))
    assert(c.outputFormat == "postgres")
    assert(c.output == "people_enriched")
  }

  test("an explicit --output is never replaced by a table default") {
    val c = PipelineConfig.parse(Array("--output-format", "postgres", "--output", "somewhere"))
    assert(c.output == "somewhere")
  }

  test("jdbc flags override the defaults, other fields untouched") {
    val c = PipelineConfig.parse(
      Array("--jdbc-url", "jdbc:postgresql://db:5432/x", "--jdbc-user", "u", "--jdbc-password", "p")
    )
    assert(c.jdbc == JdbcConfig("jdbc:postgresql://db:5432/x", "u", "p"))
  }

  test("the default JDBC URL leaves parameter types open so strings can land in jsonb") {
    assert(JdbcConfig.DefaultUrl.contains("stringtype=unspecified"))
  }

  test("an unsupported input format is rejected") {
    val e = intercept[IllegalArgumentException](PipelineConfig.parse(Array("--format", "csv")))
    assert(e.getMessage.startsWith("unsupported format: csv"))
  }

  test("an unsupported output format is rejected") {
    val e = intercept[IllegalArgumentException](PipelineConfig.parse(Array("--output-format", "json")))
    assert(e.getMessage.startsWith("unsupported output format: json"))
  }

  test("an unknown flag is rejected") {
    intercept[IllegalArgumentException](PipelineConfig.parse(Array("--nope", "x")))
  }
}
