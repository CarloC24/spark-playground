package playground
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkPostgres {

    val JdbcUrl = sys.env.getOrElse("POSTGRESURL","jdbc:postgresql://localhost:5432/playground")
    val User = sys.env.getOrElse("PGUSER", "spark")
    val Password = sys.env.getOrElse("PGPASSWORD", "spark")

    private def options: Map[String, String] = Map(
        "url" -> JdbcUrl,
        "user" -> User,
        "password" -> Password,
        "driver" -> "org.postgresql.Driver"
    )

    def session(appName: String = "spark-postgres"): SparkSession = 
    SparkSession
        .builder()
        .appName(appName)
        .master("local[*]")
        .getOrCreate()

    def read(spark: SparkSession, table: String): DataFrame = 
    spark.read.format("jdbc").options(options).option("dbtable", table).load()


    def main(args: Array[String]): Unit = {
        val spark = session()
        spark.sparkContext.setLogLevel("WARN")
        try {
            val people = read(spark, "people")
            people.printSchema()
            people.show(truncate = false)
            println(s"Read ${people.count()} rows")
        } finally {
            spark.stop()
        }
    }
}