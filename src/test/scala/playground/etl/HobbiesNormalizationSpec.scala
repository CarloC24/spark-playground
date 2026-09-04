package playground.etl

import org.apache.spark.sql.functions.col

/**
 * [[Extract.normalizeHobbies]] in isolation, on hand-made strings. The JSON and Postgres
 * readers both funnel through it, so this pins down the contract they share without
 * needing a file or a database.
 */
class HobbiesNormalizationSpec extends SparkSessionTestBase {

  private def normalize(raw: String*): Seq[Seq[Hobby]] = {
    import spark.implicits._
    raw.toSeq
      .toDF("raw")
      .select(Extract.normalizeHobbies(col("raw")).as("hobbies"))
      .as[Seq[Hobby]]
      .collect()
      .toSeq
  }

  test("an array passes through") {
    assert(normalize("""[{"hobbieName": "basketball"}, {"hobbieName": "tennis"}]""") ==
      Seq(Seq(Hobby("basketball"), Hobby("tennis"))))
  }

  test("a bare object is wrapped into a one-element array") {
    assert(normalize("""{"hobbieName": "wrestling"}""") == Seq(Seq(Hobby("wrestling"))))
  }

  test("null becomes an empty array, not null") {
    assert(normalize(null) == Seq(Seq.empty))
  }

  test("leading whitespace does not hide an array") {
    assert(normalize("""   [{"hobbieName": "chess"}]""") == Seq(Seq(Hobby("chess"))))
  }

  test("an empty array stays empty") {
    assert(normalize("[]") == Seq(Seq.empty))
  }
}
