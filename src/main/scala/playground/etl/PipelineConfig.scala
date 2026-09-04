package playground.etl

/**
 * How to reach Postgres. Shared by [[Extract.fromPostgres]] and [[Load.toPostgres]].
 *
 * @param url      JDBC URL. The default carries `stringtype=unspecified`, which is what
 *                 lets a Spark string column land in a `jsonb` column: without it the
 *                 driver sends every string as `varchar`, and Postgres refuses to assign
 *                 varchar to jsonb. With it the driver leaves the parameter type open and
 *                 Postgres infers it from the target column.
 * @param user     database user
 * @param password database password. Fine on the command line for a local playground;
 *                 anywhere real, use the `PGPASSWORD` environment fallback instead.
 */
case class JdbcConfig(url: String, user: String, password: String) {

  /** The options every JDBC read and write starts from. */
  def options: Map[String, String] = Map(
    "url"      -> url,
    "user"     -> user,
    "password" -> password,
    "driver"   -> "org.postgresql.Driver"
  )
}

object JdbcConfig {
  val DefaultUrl = "jdbc:postgresql://localhost:5432/playground?stringtype=unspecified"

  /** Matches docker-compose.yml. `PGUSER`/`PGPASSWORD` override, as they do for psql. */
  val default: JdbcConfig = JdbcConfig(
    url      = DefaultUrl,
    user     = sys.env.getOrElse("PGUSER", "spark"),
    password = sys.env.getOrElse("PGPASSWORD", "spark")
  )
}

/**
 * @param input        what to read: a file (json), a directory (parquet), or a table (postgres)
 * @param format       "json", "parquet", or "postgres"; inferred from `input` when not given
 * @param output       where to write: a directory (parquet) or a table (postgres)
 * @param outputFormat "parquet" or "postgres"
 * @param partitionBy  column to partition Parquet output by; ignored for postgres
 * @param jdbc         Postgres connection, used only when either format is "postgres"
 */
case class PipelineConfig(
    input: String,
    format: String,
    output: String,
    outputFormat: String,
    partitionBy: String,
    jdbc: JdbcConfig
)

object PipelineConfig {

  val DefaultInput        = "src/main/resources/people.json"
  val DefaultOutput       = "data/warehouse/people_enriched"
  val DefaultInputTable   = "people"
  val DefaultOutputTable  = "people_enriched"
  val DefaultPartitionBy  = "city"

  val InputFormats  = Set("json", "parquet", "postgres")
  val OutputFormats = Set("parquet", "postgres")

  private val Usage =
    """Usage: PeoplePipeline [input] [options]
      |
      |  --input <path|table>    what to read                (default: src/main/resources/people.json,
      |                                                        or the `people` table for --format postgres)
      |  --format <fmt>          json | parquet | postgres   (default: inferred from the input path)
      |  --output <dir|table>    where to write              (default: data/warehouse/people_enriched,
      |                                                        or the `people_enriched` table for postgres)
      |  --output-format <fmt>   parquet | postgres          (default: parquet)
      |  --partition-by <col>    Parquet partition column    (default: city)
      |  --jdbc-url <url>        Postgres JDBC URL           (default: jdbc:postgresql://localhost:5432/playground?stringtype=unspecified)
      |  --jdbc-user <user>      Postgres user               (default: $PGUSER, else spark)
      |  --jdbc-password <pw>    Postgres password           (default: $PGPASSWORD, else spark)
      |""".stripMargin

  /** json for a path that looks like a JSON file, parquet for anything else. */
  private def inferFormat(input: String): String =
    if (input.toLowerCase.endsWith(".json")) "json" else "parquet"

  /**
   * Hand-rolled rather than pulling in scopt for a handful of flags.
   *
   * A bare leading argument is taken as the input path, which keeps the
   * `sbt "run path/to/other.json"` form from the README working.
   */
  def parse(args: Array[String]): PipelineConfig = {
    val (positional, flags) = args.span(!_.startsWith("--"))
    if (positional.length > 1) fail(s"unexpected argument: ${positional(1)}")

    var input        = positional.headOption
    var format       = Option.empty[String]
    var output       = Option.empty[String]
    var outputFormat = "parquet"
    var partitionBy  = DefaultPartitionBy
    var jdbc         = JdbcConfig.default

    def value(rest: List[String], flag: String): String =
      rest.headOption.getOrElse(fail(s"$flag needs a value"))

    def loop(rest: List[String]): Unit = rest match {
      case Nil                     => ()
      case "--input" :: t          => input = Some(value(t, "--input")); loop(t.drop(1))
      case "--format" :: t         => format = Some(value(t, "--format")); loop(t.drop(1))
      case "--output" :: t         => output = Some(value(t, "--output")); loop(t.drop(1))
      case "--output-format" :: t  => outputFormat = value(t, "--output-format"); loop(t.drop(1))
      case "--partition-by" :: t   => partitionBy = value(t, "--partition-by"); loop(t.drop(1))
      case "--jdbc-url" :: t       => jdbc = jdbc.copy(url = value(t, "--jdbc-url")); loop(t.drop(1))
      case "--jdbc-user" :: t      => jdbc = jdbc.copy(user = value(t, "--jdbc-user")); loop(t.drop(1))
      case "--jdbc-password" :: t  => jdbc = jdbc.copy(password = value(t, "--jdbc-password")); loop(t.drop(1))
      case other :: _              => fail(s"unknown option: $other")
    }
    loop(flags.toList)

    // The input format is inferred from the path, so it has to be settled before the
    // default input is chosen: a table name has no extension to infer from.
    val fmt = format.getOrElse(input.map(inferFormat).getOrElse("json"))
    if (!InputFormats(fmt)) fail(s"unsupported format: $fmt")
    if (!OutputFormats(outputFormat)) fail(s"unsupported output format: $outputFormat")

    val in  = input.getOrElse(if (fmt == "postgres") DefaultInputTable else DefaultInput)
    val out = output.getOrElse(if (outputFormat == "postgres") DefaultOutputTable else DefaultOutput)

    PipelineConfig(in, fmt, out, outputFormat, partitionBy, jdbc)
  }

  private def fail(message: String): Nothing =
    throw new IllegalArgumentException(s"$message\n\n$Usage")
}
