package playground.etl

/**
 * Exercises the JSON reader against the real `people.json`, which deliberately spells the
 * key both ways and shapes the value both ways — four of the five combinations appear.
 */
class ExtractSpec extends SparkSessionTestBase {

  private lazy val people: Seq[Person] =
    Extract.fromJson(spark, peopleJson).collect().sortBy(_.id).toSeq

  test("reads every record") {
    assert(people.length == 5)
  }

  test("hobbies is never null, whatever the key spelling or value shape") {
    assert(people.forall(_.hobbies != null))
  }

  test("an array under the uppercase key survives intact") {
    // id 1: "Hobbies": [{...}, {...}]
    assert(people.head.hobbies == Seq(Hobby("basketball"), Hobby("tennis")))
  }

  test("a bare object under the uppercase key becomes a one-element array") {
    // id 2: "Hobbies": {"hobbieName": "wrestling"}
    assert(people(1).hobbies == Seq(Hobby("wrestling")))
  }

  test("an array under the lowercase key survives intact") {
    // id 3: "hobbies": [{...}, {...}]
    assert(people(2).hobbies == Seq(Hobby("basketball"), Hobby("tennis")))
  }

  test("a bare object under the lowercase key becomes a one-element array") {
    // id 4: "hobbies": {"hobbieName": "wrestling"}
    assert(people(3).hobbies == Seq(Hobby("wrestling")))
  }

  test("the scalar fields come through") {
    val amara = people.head
    assert(amara.name == "Amara Okonkwo")
    assert(amara.age == 34)
    assert(amara.city == "Lagos")
  }
}
