package playground.etl

import org.apache.spark.sql.functions.{col, size, substring_index, when}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * The T of ETL. Format-agnostic by construction: [[Extract]] has already guaranteed a
 * uniform `Dataset[Person]`, so there is nothing here about JSON, Parquet, or the
 * `hobbies` key mess.
 */
object Transform {

  /** Adds `ageGroup`, `emailDomain`, and `hobbyCount` to each record. */
  def apply(spark: SparkSession, people: Dataset[Person]): Dataset[PersonEnriched] = {
    import spark.implicits._

    people
      .select(
        col("id"),
        col("name"),
        col("email"),
        col("age"),
        col("city"),
        col("hobbies"),
        when(col("age") < 35, "under-35").otherwise("35-plus").as("ageGroup"),
        // Column expression rather than a typed `map` with `email.split("@").last`:
        // on an address with no "@" that Scala version returns the whole string too,
        // but on an empty string it throws, and on "a@" it silently yields "". This
        // degrades predictably.
        substring_index(col("email"), "@", -1).as("emailDomain"),
        size(col("hobbies")).as("hobbyCount")
      )
      .as[PersonEnriched]
  }
}
