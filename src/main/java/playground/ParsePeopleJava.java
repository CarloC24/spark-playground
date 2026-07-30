package playground;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/** Java port of {@code playground.ParsePeople}. Reads 5 person records from JSON. */
public final class ParsePeopleJava {

    private static final String DEFAULT_INPUT = "src/main/resources/people.json";

    /**
     * Declared by hand rather than read off the bean encoder. {@code Introspector}
     * returns properties alphabetically, so {@code Encoders.bean(...).schema()} would
     * print the columns in a different order than the Scala version. Being explicit
     * also keeps {@code age} an int instead of letting inference widen it to a long.
     */
    private static final StructType SCHEMA = new StructType()
            .add("id", DataTypes.LongType)
            .add("name", DataTypes.StringType)
            .add("email", DataTypes.StringType)
            .add("age", DataTypes.IntegerType)
            .add("city", DataTypes.StringType);

    private ParsePeopleJava() {}

    public static void main(String[] args) {
        String input = args.length > 0 ? args[0] : DEFAULT_INPUT;

        SparkSession spark = SparkSession.builder()
                .appName("parse-people-java")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Scala gets this from `import spark.implicits._`; Java has to be explicit.
        Encoder<PersonBean> personEncoder = Encoders.bean(PersonBean.class);

        Dataset<PersonBean> people = spark.read()
                // The file is a pretty-printed JSON array, not one object per line.
                .option("multiLine", true)
                .schema(SCHEMA)
                .json(input)
                .as(personEncoder);

        System.out.printf("Parsed %d person records from %s%n", people.count(), input);
        people.printSchema();
        people.show(false);

        // Proof the rows really are typed PersonBean objects, not just Rows.
        for (PersonBean p : people.collectAsList()) {
            System.out.printf("%d: %s (%d) — %s%n", p.getId(), p.getName(), p.getAge(), p.getCity());
        }

        spark.stop();
    }
}
