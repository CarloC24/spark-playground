# spark-playground

A minimal Scala + Spark project that reads a JSON file of 5 person records into a
typed `Dataset[Person]` and prints it.

| | |
|---|---|
| Scala | 2.13.18 |
| Spark | 4.2.0 |
| sbt | 1.12.14 |
| JDK | 21 (Spark 4 supports 17 and 21 only) |

## Run

```sh
sbt run
```

Or point it at a different file:

```sh
sbt "run path/to/other.json"
```

Expected output:

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
build.sbt                              deps + JDK 17/21 --add-opens flags
.sbtopts                               pins the build to JDK 21
project/build.properties               sbt version
src/main/resources/people.json         the 5 records
src/main/resources/log4j2.properties   quiets Spark's startup logging
src/main/scala/playground/ParsePeople.scala
```

## Notes on the setup

**JDK.** Spark 4.2 targets Java 17; 21 is the newest version it supports. This machine's
default `java` is 26, which Spark will not run on, so `.sbtopts` pins the build to a
keg-only Homebrew JDK 21:

```
-java-home /usr/local/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
```

Your global `java` is untouched. If that path moves, update `.sbtopts`.

**`--add-opens` flags.** Spark reads JDK internals that the module system closed off in
Java 17+. `build.sbt` passes the standard set of `--add-opens` flags and sets
`run / fork := true` — the flags only apply to a forked JVM, so without the fork the job
crashes on startup.

**`multiLine`.** `people.json` is a pretty-printed JSON array. Spark's JSON reader
defaults to JSON Lines (one object per line), so the code sets
`.option("multiLine", true)`. Drop that option if you switch the file to JSON Lines.

**Explicit schema.** The reader is given `Encoders.product[Person].schema` rather than
inferring. Inference would type `age` as `long`; the explicit schema keeps it `int` to
match the case class, and means a malformed file fails loudly instead of silently
producing a different shape.
