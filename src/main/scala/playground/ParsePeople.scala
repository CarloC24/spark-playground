package playground

import org.apache.spark.sql.functions.{
  coalesce,
  col,
  concat,
  from_json,
  lit,
  size,
  substring_index,
  to_json,
  trim,
  when
}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Hobby(hobbieName: String)

case class Person(
    id: Long,
    name: String,
    email: String,
    age: Int,
    city: String,
    hobbies: Seq[Hobby]
)

/** A reshaped view of [[Person]]: fewer fields, two of them derived. */
case class PersonSummary(
    name: String,
    ageGroup: String,
    emailDomain: String
)

object ParsePeople {

  private val defaultInput = "src/main/resources/people.json"

  /**
   * Renders hobbies for the console. Only for the println loop below, which holds
   * plain Scala objects with no Spark column to hand to `to_json`; the DataFrame
   * display uses the real `to_json`.
   */
  private def asJson(hobbies: Seq[Hobby]): String =
    hobbies.map(h => s"""{"hobbieName":"${h.hobbieName}"}""").mkString("[", ",", "]")

  /** The shape every row's hobbies column ends up in, no matter what the JSON held. */
  private val hobbiesType = ArrayType(new StructType().add("hobbieName", StringType))

  /**
   * Read schema. Two things here are deliberate and both are forced on us by the data:
   *
   *  - `hobbies` is declared twice, once per spelling. JacksonParser resolves JSON keys
   *    with `StructType.getFieldIndex`, which is case-sensitive, and it ignores
   *    `spark.sql.caseSensitive` entirely. Declaring only one spelling silently yields
   *    null for the rows that use the other.
   *  - Both are `StringType`, not the real array type. An `ArrayType` converter only
   *    accepts a START_ARRAY token, so the rows holding a bare `{...}` would parse as
   *    null. Asking for a string instead makes the parser hand back the raw JSON source
   *    text, which is the one representation both shapes survive as.
   */
  private val readSchema = new StructType()
    .add("id", LongType)
    .add("name", StringType)
    .add("email", StringType)
    .add("age", IntegerType)
    .add("city", StringType)
    .add("hobbies", StringType)
    .add("Hobbies", StringType)

  def main(args: Array[String]): Unit = {
    val input = args.headOption.getOrElse(defaultInput)

    val spark = SparkSession
      .builder()
      .appName("parse-people")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // Required so `hobbies` and `Hobbies` can coexist as distinct columns; under the
    // default (false) they collide as duplicate names.
    //
    // This has to stay on for the life of the read, not just around the select below.
    // FileSourceStrategy re-resolves the required columns against the file schema
    // during *physical planning*, which happens lazily at action time — flipping the
    // flag back after building the Dataset gets you AMBIGUOUS_REFERENCE on the first
    // count()/show(), long after the plan looked fine.
    spark.conf.set("spark.sql.caseSensitive", "true")

    val raw: DataFrame = spark.read
      // The file is a pretty-printed JSON array, not one object per line.
      .option("multiLine", value = true)
      .schema(readSchema)
      .json(input)

    // Whichever spelling this row used, as raw JSON text.
    val rawHobbies = coalesce(col("hobbies"), col("Hobbies"))

    // Force it to array-of-objects text: pass an array through, wrap a bare object,
    // and treat a missing key as empty rather than null so downstream code can always
    // assume a list.
    val hobbiesJson = when(rawHobbies.isNull, lit("[]"))
      .when(trim(rawHobbies).startsWith("["), rawHobbies)
      .otherwise(concat(lit("["), rawHobbies, lit("]")))

    val people: Dataset[Person] = raw
      .select(
        col("id"),
        col("name"),
        col("email"),
        col("age"),
        col("city"),
        from_json(hobbiesJson, hobbiesType).as("hobbies")
      )
      .as[Person]
    println("spark project")
    println(s"Parsed ${people.count()} person records from $input")
    people.printSchema()

    // show() renders a struct as {basketball}: positional values with the field names
    // stripped. to_json prints the column as the JSON it actually is, so the objects
    // come out as {"hobbieName":"basketball"} rather than a bare value.
    people.withColumn("hobbies", to_json(col("hobbies"))).show(truncate = false)

    // Proof the rows really are typed Person objects, not just Rows.
    people.collect().foreach { p =>
      println(s"${p.id}: ${p.name} (${p.age}) — ${p.city} — hobbies: ${asJson(p.hobbies)}")
    }

    // The whole point: every row is an array now, regardless of how the JSON spelled
    // the key or shaped the value. Nothing should be null, and nothing a bare struct.
    val notAnArray = people.filter(col("hobbies").isNull).count()
    println(s"Rows where hobbies is not an array: $notAnArray")
    println("Hobby counts per row: " + people.select(size(col("hobbies")))
      .collect().map(_.getInt(0)).mkString(", "))

    // ---- a second, reshaped frame built from the same records ----

    // Typed route: map each Person to a PersonSummary. The compiler checks the
    // shape, and `import spark.implicits._` supplies the encoder for the result.
    val summaries: Dataset[PersonSummary] = people.map { p =>
      PersonSummary(
        name = p.name,
        ageGroup = if (p.age < 35) "under-35" else "35-plus",
        emailDomain = p.email.split("@").last
      )
    }

    println("Summaries (typed, via map):")
    summaries.printSchema()
    summaries.show(truncate = false)

    // Untyped route: the same frame from column expressions. No case class needed,
    // but a typo in a column name only blows up at runtime.
    val summariesDf: DataFrame = people.toDF().select(
      col("name"),
      when(col("age") < 35, "under-35").otherwise("35-plus").as("ageGroup"),
      substring_index(col("email"), "@", -1).as("emailDomain")
    )

    println("Summaries (untyped, via select):")
    summariesDf.show(truncate = false)

    spark.stop()
  }
}
