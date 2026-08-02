package playground.etl

/** In-memory only — [[Transform]] never touches a file, so neither does this. */
class TransformSpec extends SparkSessionTestBase {

  private def person(
      id: Long,
      age: Int = 30,
      email: String = "someone@example.com",
      hobbies: Seq[Hobby] = Seq(Hobby("chess"))
  ): Person = Person(id, s"person-$id", email, age, "Somewhere", hobbies)

  private def enrich(people: Person*): Map[Long, PersonEnriched] = {
    import spark.implicits._
    Transform(spark, spark.createDataset(people))
      .collect()
      .map(p => p.id -> p)
      .toMap
  }

  test("ageGroup splits at 35, with 35 itself on the older side") {
    val out = enrich(person(1, age = 34), person(2, age = 35), person(3, age = 52))
    assert(out(1).ageGroup == "under-35")
    assert(out(2).ageGroup == "35-plus")
    assert(out(3).ageGroup == "35-plus")
  }

  test("emailDomain is everything after the last @") {
    val out = enrich(person(1, email = "chen.wei@example.com"))
    assert(out(1).emailDomain == "example.com")
  }

  test("an address with no @ yields itself rather than blowing up") {
    val out = enrich(person(1, email = "not-an-address"))
    assert(out(1).emailDomain == "not-an-address")
  }

  test("hobbyCount counts the array, including the empty case") {
    val out = enrich(
      person(1, hobbies = Seq.empty),
      person(2, hobbies = Seq(Hobby("tennis"))),
      person(3, hobbies = Seq(Hobby("basketball"), Hobby("tennis")))
    )
    assert(out(1).hobbyCount == 0)
    assert(out(2).hobbyCount == 1)
    assert(out(3).hobbyCount == 2)
  }

  test("the Person fields pass through untouched") {
    val out = enrich(person(7, age = 41, hobbies = Seq(Hobby("wrestling"))))
    assert(out(7).name == "person-7")
    assert(out(7).city == "Somewhere")
    assert(out(7).hobbies == Seq(Hobby("wrestling")))
  }
}
