package playground.etl

/**
 * @param input       file (JSON) or directory (Parquet) to read
 * @param format      "json" or "parquet"; inferred from `input` when not given
 * @param output      directory to write partitioned Parquet into
 * @param partitionBy column to partition the output by
 */
case class PipelineConfig(
    input: String,
    format: String,
    output: String,
    partitionBy: String
)

object PipelineConfig {

  val DefaultInput       = "src/main/resources/people.json"
  val DefaultOutput      = "data/warehouse/people_enriched"
  val DefaultPartitionBy = "city"

  private val Usage =
    """Usage: PeoplePipeline [input] [options]
      |
      |  --input <path>          file or directory to read   (default: src/main/resources/people.json)
      |  --format <fmt>          json | parquet              (default: inferred from the input path)
      |  --output <dir>          where to write Parquet      (default: data/warehouse/people_enriched)
      |  --partition-by <col>    output partition column     (default: city)
      |""".stripMargin

  /** json for a path that looks like a JSON file, parquet for anything else. */
  private def inferFormat(input: String): String =
    if (input.toLowerCase.endsWith(".json")) "json" else "parquet"

  /**
   * Hand-rolled rather than pulling in scopt for four flags.
   *
   * A bare leading argument is taken as the input path, which keeps the
   * `sbt "run path/to/other.json"` form from the README working.
   */
  def parse(args: Array[String]): PipelineConfig = {
    val (positional, flags) = args.span(!_.startsWith("--"))
    if (positional.length > 1) fail(s"unexpected argument: ${positional(1)}")

    var input       = positional.headOption
    var format      = Option.empty[String]
    var output      = DefaultOutput
    var partitionBy = DefaultPartitionBy

    def value(rest: List[String], flag: String): String =
      rest.headOption.getOrElse(fail(s"$flag needs a value"))

    def loop(rest: List[String]): Unit = rest match {
      case Nil                     => ()
      case "--input" :: t          => input = Some(value(t, "--input")); loop(t.drop(1))
      case "--format" :: t         => format = Some(value(t, "--format")); loop(t.drop(1))
      case "--output" :: t         => output = value(t, "--output"); loop(t.drop(1))
      case "--partition-by" :: t   => partitionBy = value(t, "--partition-by"); loop(t.drop(1))
      case other :: _              => fail(s"unknown option: $other")
    }
    loop(flags.toList)

    val in  = input.getOrElse(DefaultInput)
    val fmt = format.getOrElse(inferFormat(in))
    if (fmt != "json" && fmt != "parquet") fail(s"unsupported format: $fmt")

    PipelineConfig(in, fmt, output, partitionBy)
  }

  private def fail(message: String): Nothing =
    throw new IllegalArgumentException(s"$message\n\n$Usage")
}
