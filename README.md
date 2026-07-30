# spark-playground

The same Spark job written twice — once in Scala, once in Java. Both read a JSON file
of 5 person records into a typed `Dataset` and print it, and both produce identical
output.

| | Scala | Java |
|---|---|---|
| Entry point | `playground.ParsePeople` | `playground.ParsePeopleJava` |
| Record type | `Person` (case class) | `PersonBean` (JavaBean) |
| Build tool | sbt 1.12.14 | Maven 3.9 |
| Sources | `src/main/scala/` | `src/main/java/` |
| Build output | `target/` | `maven-target/` |

Shared by both: Spark 4.2.0, JDK 21, and `src/main/resources/` (the JSON file and the
log4j2 config).

## Run

Scala:

```sh
sbt run
sbt "run path/to/other.json"
```

Java:

```sh
mvn compile exec:exec
mvn compile exec:exec -Dinput.json=path/to/other.json
```

Both print:

```
Parsed 5 person records from src/main/resources/people.json
root
 |-- id: long (nullable = true)
 |-- name: string (nullable = true)
 |-- email: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- city: string (nullable = true)
+---+---------------+---------------------------+---+---------+
|id |name           |email                      |age|city     |
+---+---------------+---------------------------+---+---------+
|1  |Amara Okonkwo  |amara.okonkwo@example.com  |34 |Lagos    |
...
```

## Layout

```
build.sbt                                       Scala build
pom.xml                                         Java build
.sbtopts                                        pins sbt to JDK 21
project/build.properties                        sbt version
src/main/resources/people.json                  the 5 records (shared)
src/main/resources/log4j2.properties            quiets Spark's logging (shared)
src/main/scala/playground/ParsePeople.scala
src/main/java/playground/ParsePeopleJava.java
src/main/java/playground/PersonBean.java
```

## Scala vs. Java: what actually differs

**Encoders.** Scala gets one for free from `import spark.implicits._`, derived from the
case class at compile time. Java has to ask for it explicitly with
`Encoders.bean(PersonBean.class)`.

**The record type is the big one.** `PersonBean` is a mutable class with a no-arg
constructor, getters, setters, and hand-written `equals`/`hashCode`/`toString` — 94 lines
against the case class's 7-line declaration, which supplies all of that. It cannot
be a Java `record`: `Encoders.bean` discovers fields via `java.beans.Introspector`, which
looks for `getX()`/`setX()` pairs, and a record exposes `x()` accessors with no setters.
The introspector finds no properties and the encoder fails. There is no record support in
Spark 4.2's `JavaTypeInference`.

**Schema declaration.** Both declare the schema explicitly rather than inferring it.
Scala derives it from the case class (`Encoders.product[Person].schema`), which preserves
field order. Java builds a `StructType` by hand, because `Introspector` returns properties
alphabetically — using `Encoders.bean(...).schema()` would print the columns as
age, city, email, id, name instead of the declared order.

Everything else — `multiLine`, `local[*]`, `printSchema`, `show`, the collect loop — maps
across essentially one-to-one.

## Notes on the setup

**JDK.** Spark 4.2 targets Java 17; 21 is the newest it supports. This machine's default
`java` is 26, which Spark will not run on, so both builds pin JDK 21 explicitly:

- sbt, via `.sbtopts`: `-java-home /usr/local/opt/openjdk@21/...`
- Maven, via the `spark.java.home` property in `pom.xml`, used as the `exec` executable.
  Override with `mvn ... -Dspark.java.home=/path/to/jdk`.

Your global `java` is untouched. If the Homebrew path moves, update both.

Note that Maven itself still runs on JDK 26 — only the Spark JVM needs to be 21.
Compilation targets `release 21`, which works fine under a newer `javac`.

**`--add-opens` flags.** Spark reads JDK internals that the module system closed off in
Java 17+. Both builds pass the standard set, and both run Spark in a separate JVM for
that reason — `run / fork := true` in sbt, and `exec:exec` rather than `exec:java` in
Maven (the latter would reuse Maven's own JVM, where the flags don't apply).

**Two build tools, one repo.** They deliberately do not overlap:

- Maven's `<directory>` is `maven-target`, not `target`. Otherwise `mvn clean` would
  delete the Scala build's output, and `sbt clean` the Java one's.
- sbt's `Compile / unmanagedSourceDirectories` is narrowed to `src/main/scala`. By
  default sbt also compiles `src/main/java`, which makes `sbt run` fail with
  "Multiple main classes detected" once a Java main class exists.

**`multiLine`.** `people.json` is a pretty-printed JSON array. Spark's JSON reader
defaults to JSON Lines (one object per line), so both versions set
`multiLine = true`. Drop it if you switch the file to JSON Lines.
