# spark-playground

Three things live here.

**An ETL pipeline** (`playground.etl`, Scala) — reads person records from JSON, from a
Hive-partitioned Parquet directory, *or* from a Postgres table, derives a few columns, and
writes partitioned Parquet back out. This is what `sbt run` runs. Postgres runs in Docker
(`docker-compose.yml`).

**The same small job written twice**, once in Scala and once in Java — a side-by-side
language comparison, and where the JSON parsing tricks the pipeline reuses were worked out.

**A project plan** — [`docs/lakehouse-project.md`](docs/lakehouse-project.md) lays out what to
build next on top of this: a mini lakehouse table format on plain Parquet (a transaction
log, file skipping, a DataSource V2 write path, upserts, time travel, a SQL catalog), in six
milestones, using nothing but Spark and Scala.

Everything shares Spark 4.2.0, JDK 21, and `src/main/resources/`.

## ETL pipeline

```sh
sbt test    # 35 tests; the 4 Postgres ones cancel themselves if the database is not up
sbt run     # people.json -> data/warehouse/people_enriched/city=*/
```

Then read that output back — the same pipeline, now on the Parquet path:

```sh
sbt "run --input data/warehouse/people_enriched --output data/warehouse/people_reprocessed"
```

And the same pipeline reading from Postgres. Start the database first; the first start also
creates and seeds the `people` table from `docker/postgres/init.sql`:

```sh
docker compose up -d
sbt "run --format postgres"     # people table -> data/warehouse/people_enriched/city=*/
docker compose exec postgres psql -U spark -d playground -c 'SELECT * FROM people ORDER BY id'
```

| Option | Default | |
|---|---|---|
| `--input <path\|table>` | `src/main/resources/people.json`, or `people` for postgres | file (JSON), directory (Parquet), or table (Postgres) |
| `--format json\|parquet\|postgres` | inferred from the input path | postgres is never inferred |
| `--output <dir>` | `data/warehouse/people_enriched` | |
| `--partition-by <col>` | `city` | any column of the output |
| `--jdbc-url <url>` | `jdbc:postgresql://localhost:5432/playground` | |
| `--jdbc-user`, `--jdbc-password` | `$PGUSER` / `$PGPASSWORD`, else `spark` / `spark` | matches `docker-compose.yml` |

A bare first argument is still taken as the input path, so `sbt "run other.json"` works.

### The one design decision

`Extract` is the only stage that knows where rows come from. All three of its readers return
the same thing — a `Dataset[Person]` whose `hobbies` is always a non-null array — so
`Transform` and `Load` never learn where the rows came from.

That split exists because the `hobbies` mess is a **JSON-only problem**. JacksonParser
resolves keys case-sensitively and its `ArrayType` converter rejects a bare `{...}`, so the
JSON reader has to declare both spellings as `StringType` and reshape the raw text with
`from_json` (`Schemas.jsonReadSchema` carries the full reasoning). Parquet has neither
problem — it carries its own schema and stores `hobbies` already typed as an array of
structs. Postgres sits in between: the column name is fixed, so the key-spelling problem is gone,
but `jsonb` arrives in Spark as a string and can hold a bare object, so it shares the JSON
reader's reshaping step (`Extract.normalizeHobbies`). Normalizing in `Extract` rather than
`Transform` is what lets one transform serve every source.

`PipelineRoundTripSpec` is what holds that in place: JSON → Parquet → back, and the two
`Dataset[Person]`s must come out equal. `PostgresExtractSpec` does the same for the
database: JSON and the `people` table must extract equal.

### Stages

| | |
|---|---|
| `PipelineConfig` | seven flags, hand-parsed; no scopt. `JdbcConfig` holds the Postgres connection |
| `Schemas` | `Person`, `PersonEnriched`, and the JSON read schema |
| `Extract` | `fromJson` normalizes; `fromParquet` selects the canonical columns; `fromPostgres` does a partitioned JDBC read and normalizes |
| `Transform` | adds `ageGroup`, `emailDomain`, `hobbyCount` |
| `Load` | `Overwrite` + `partitionBy`, one file per partition value |

Two details that bite if you change them:

- **`spark.sql.caseSensitive` is set on the session builder**, not with a later
  `spark.conf.set`, and has to stay on for the life of the read. It is what lets `hobbies`
  and `Hobbies` coexist as distinct columns. `FileSourceStrategy` re-resolves the required
  columns during *physical* planning, which is lazy, so turning it back off after building
  the Dataset gets you `AMBIGUOUS_REFERENCE` at the first action, long after the plan
  looked fine.
- **`Extract.fromParquet` selects columns by name** rather than taking the frame as-is.
  Partition discovery appends `city` at the *end* of the schema no matter where it sat
  when written, and re-reading the pipeline's own output drags the enrichment columns
  along too.

### Postgres

`docker-compose.yml` runs `postgres:17-alpine` with a database `playground` and user
`spark`/`spark`. `docker/postgres/init.sql` runs once, on the first start of an empty data
volume, and creates the `people` table, seeded with the same 5 records as `people.json`.
`docker compose down -v` wipes the volume so the script runs again.

The table stores `hobbies` as `jsonb`, and deliberately in both shapes the JSON file uses
(an array of objects, or a bare object), so the reader has the same reshaping to do.

**Reading** (`Extract.fromPostgres`) goes through Spark's built-in JDBC source and its
`PostgresDialect`, which maps `bigint`/`integer`/`text`/`jsonb` to `Long`/`Int`/`String`/
`String`. Two things the obvious `spark.read.jdbc` would get wrong:

- A plain JDBC read runs on **one task**, whatever the cluster looks like. Splitting it
  needs a numeric `partitionColumn` plus `lowerBound`/`upperBound`, which Spark does not
  discover for you, so the reader first runs a tiny `SELECT min(id), max(id)` and then
  issues one range query per partition. Four partitions for 5 rows is silly on purpose:
  the empty ones are visible in the Spark UI rather than hidden.
- With the default `fetchsize` of 0 the Postgres driver materializes the whole result set
  before returning the first row. Any positive value makes it stream with a cursor.

`PostgresExtractSpec` needs the Compose database. When it cannot connect, its tests are
**canceled**, not failed, so `sbt test` stays green without Docker. `PG_JDBC_URL`, `PGUSER`
and `PGPASSWORD` point it at another server.

Deliberately not built, since this is a playground: `_corrupt_record` quarantine,
data-quality assertions, and `partitionOverwriteMode=dynamic` for per-partition rather than
whole-directory overwrite.

Also not built yet: a Postgres **sink**. Writing back through Spark's JDBC writer needs three
things the reader does not. `hobbies` has to go over as JSON text (`to_json`), because the
dialect can write arrays of primitives but not arrays of structs, and the URL then needs
`stringtype=unspecified` for that string to land in a `jsonb` column. Column names should be
snake_case, because Spark double-quotes identifiers and an `ageGroup` column would become a
case-sensitive `"ageGroup"` that every query has to quote. And `Overwrite` should carry
`truncate=true`, so reruns keep the table's DDL instead of dropping and recreating it.

## Scala vs. Java

Both read a JSON file of 5 person records into a typed `Dataset` and print it. The Java
port stops at the five scalar fields; only the Scala one normalizes `hobbies`.

| | Scala | Java |
|---|---|---|
| Entry point | `playground.ParsePeople` | `playground.ParsePeopleJava` |
| Record type | `Person` (case class) | `PersonBean` (JavaBean) |
| Build tool | sbt 1.12.14 | Maven 3.9 |
| Sources | `src/main/scala/` | `src/main/java/` |
| Build output | `target/` | `maven-target/` |

### Run

Scala — `sbt run` belongs to the pipeline now, so the demo needs `runMain`:

```sh
sbt "runMain playground.ParsePeople"
```

Java:

```sh
mvn compile exec:exec
mvn compile exec:exec -Dinput.json=path/to/other.json
```

Both print a schema and a table; the Scala one carries a sixth `hobbies` column:

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

### What actually differs

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

## Layout

```
build.sbt                                       Scala build
pom.xml                                         Java build
.sbtopts                                        pins sbt to JDK 21
project/build.properties                        sbt version
docker-compose.yml                              Postgres for the pipeline
docker/postgres/init.sql                        creates and seeds the people table
docs/lakehouse-project.md                       the next project: a lakehouse format on Parquet
src/main/resources/people.json                  the 5 records (shared)
src/main/resources/log4j2.properties            quiets Spark's logging (shared)
src/main/scala/playground/etl/                  the ETL pipeline
src/main/scala/playground/ParsePeople.scala     Scala demo
src/main/java/playground/ParsePeopleJava.java   Java demo
src/main/java/playground/PersonBean.java
src/test/scala/playground/etl/                  pipeline tests
data/                                           pipeline output (gitignored)
```

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
- `Compile / run / mainClass` pins `sbt run` to the pipeline. That is the same "Multiple
  main classes detected" failure from the other direction — there are two Scala mains now.
  `sbt "runMain playground.ParsePeople"` still reaches the demo.

**`multiLine`.** `people.json` is a pretty-printed JSON array. Spark's JSON reader
defaults to JSON Lines (one object per line), so both versions set
`multiLine = true`. Drop it if you switch the file to JSON Lines.
