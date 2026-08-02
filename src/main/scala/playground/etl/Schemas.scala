package playground.etl

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructType}

case class Hobby(hobbieName: String)

case class Person(
    id: Long,
    name: String,
    email: String,
    age: Int,
    city: String,
    hobbies: Seq[Hobby]
)

/** [[Person]] plus the three columns [[Transform]] derives. What the pipeline writes. */
case class PersonEnriched(
    id: Long,
    name: String,
    email: String,
    age: Int,
    city: String,
    hobbies: Seq[Hobby],
    ageGroup: String,
    emailDomain: String,
    hobbyCount: Int
)

/**
 * Schema definitions shared by both readers.
 *
 * The contract every reader in [[Extract]] must honour: a `Dataset[Person]` whose
 * `hobbies` is a non-null array. Getting there is cheap for Parquet and expensive
 * for JSON, which is why the work lives per-format rather than in [[Transform]].
 */
object Schemas {

  /** The shape every row's hobbies column ends up in, no matter what the JSON held. */
  val hobbiesType: ArrayType = ArrayType(new StructType().add("hobbieName", StringType))

  /**
   * JSON read schema. Two things here are deliberate and both are forced on us by the data:
   *
   *  - `hobbies` is declared twice, once per spelling. JacksonParser resolves JSON keys
   *    with `StructType.getFieldIndex`, which is case-sensitive, and it ignores
   *    `spark.sql.caseSensitive` entirely. Declaring only one spelling silently yields
   *    null for the rows that use the other.
   *  - Both are `StringType`, not the real array type. An `ArrayType` converter only
   *    accepts a START_ARRAY token, so the rows holding a bare `{...}` would parse as
   *    null. Asking for a string instead makes the parser hand back the raw JSON source
   *    text, which is the one representation both shapes survive as.
   *
   * None of this applies to Parquet, which carries its own schema and stores `hobbies`
   * already typed as an array of structs.
   */
  val jsonReadSchema: StructType = new StructType()
    .add("id", LongType)
    .add("name", StringType)
    .add("email", StringType)
    .add("age", IntegerType)
    .add("city", StringType)
    .add("hobbies", StringType)
    .add("Hobbies", StringType)

  /** Canonical column order, matching [[Person]]'s declaration. */
  val personColumns: Seq[Column] =
    Seq("id", "name", "email", "age", "city", "hobbies").map(col)
}
