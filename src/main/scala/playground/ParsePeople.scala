package playground

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

case class Person(
    id: Long,
    name: String,
    email: String,
    age: Int,
    city: String
)

object ParsePeople {

  private val defaultInput = "src/main/resources/people.json"

  def main(args: Array[String]): Unit = {
    val input = args.headOption.getOrElse(defaultInput)

    val spark = SparkSession
      .builder()
      .appName("parse-people")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val people: Dataset[Person] = spark.read
      // The file is a pretty-printed JSON array, not one object per line.
      .option("multiLine", value = true)
      // An explicit schema keeps types stable instead of letting inference guess.
      .schema(Encoders.product[Person].schema)
      .json(input)
      .as[Person]

    println(s"Parsed ${people.count()} person records from $input")
    people.printSchema()
    people.show(truncate = false)

    // Proof the rows really are typed Person objects, not just Rows.
    people.collect().foreach { p =>
      println(s"${p.id}: ${p.name} (${p.age}) — ${p.city}")
    }

    spark.stop()
  }
}
