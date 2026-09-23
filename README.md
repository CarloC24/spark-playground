# spark-playground

A local Spark playground: Apache Spark 4.2 and Scala 2.13, reading from a Postgres 17
container. No cluster, no cloud — everything runs in one JVM on your machine.

## Requirements

| | |
|---|---|
| JDK 21 | pinned in `.sbtopts` to the Homebrew `openjdk@21` |
| sbt 1.12.14 | pinned in `project/build.properties` |
| Docker Desktop | must be started manually before `docker compose` works |

## Quick start

```bash
open -a Docker              # macOS: Docker Desktop does not start itself
docker compose up -d        # Postgres 17 on :5432, seeded from docker/postgres/init.sql
sbt run                     # reads the `people` table and prints it
```

Expected output: the schema of `people`, five rows, and `Read 5 rows`.

## Running things

`build.sbt` pins the default entry point, so a bare `run` is unambiguous:

```scala
Compile / run / mainClass := Some("playground.SparkPostgres")
```

| Command | What it does |
|---|---|
| `sbt run` | runs `playground.SparkPostgres` |
| `sbt "runMain playground.Other"` | runs a specific class, ignoring the pin |
| `sbt compile` | compiles without running |
| `sbt console` | Scala REPL with the project classpath |
| `sbt "show discoveredMainClasses"` | lists every class with a `main` |

There are no tests yet — see *Next steps* below.

Two settings in `build.sbt` are load-bearing and easy to lose:

- **`run / fork := true`** — Spark needs the `--add-opens` flags listed in `sparkJavaOptions`,
  and JVM flags only apply to a forked process. Without the fork, Spark dies at startup with
  `InaccessibleObjectException`.
- **the `org.postgresql` dependency** — Spark ships the JDBC data source and a
  `PostgresDialect`, but not the driver. Missing it fails at runtime with `No suitable
  driver`, never at compile time.

## The database

Credentials are deliberately trivial; this is a local playground, not a deployment. The
defaults in `SparkPostgres.scala` match `docker-compose.yml`, so nothing needs configuring.

| | |
|---|---|
| JDBC URL | `jdbc:postgresql://localhost:5432/playground` |
| user / password | `spark` / `spark` |
| table | `people` — 5 rows, seeded by `docker/postgres/init.sql` |

Override with the `POSTGRESURL`, `PGUSER` and `PGPASSWORD` environment variables.

```bash
docker compose exec postgres psql -U spark -d playground   # a shell on the database
docker compose down                                        # stop, keep the data
docker compose down -v                                     # stop and wipe, so init.sql reruns
```

`init.sql` only runs when the `pgdata` volume is empty. If you change it and see no effect,
you want `down -v`.

Note the `hobbies` column: it is `jsonb`, and the values are stored in two different shapes —
an array of objects for ids 1 and 3, a bare object for 2, 4 and 5. Spark maps `jsonb` to
`StringType`, so anything reading it has to handle both shapes. That is deliberate.

## Layout

```
build.sbt                                      Spark 4.2.0, Scala 2.13.18, Postgres JDBC 42.7.7
.sbtopts                                       JDK 21
project/build.properties                       sbt 1.12.14
src/main/scala/playground/SparkPostgres.scala  session + JDBC read
docker-compose.yml                             Postgres 17
docker/postgres/init.sql                       the `people` table and its 5 rows
docs/                                          working notes — git-ignored, local only
```

## Next steps

The plan is to build a mini lakehouse — a table format with atomic commits, snapshots, time
travel and upserts, on plain Parquet, using nothing but what Spark already ships.

Working notes live in `docs/`, which is git-ignored, so they exist only on the machine that
wrote them:

| | |
|---|---|
| `next-steps.md` | what exists, the four steps from here, and what each looks like when done |
| `spark-postgres-session.md` | how the Spark + Postgres setup was built, and the JDBC traps |
| `spark-testing-setup.md` | adding scalatest — the next thing to do |
| `lakehouse-project.md` | the full six-milestone lakehouse project |

The pre-reset version of this repo had a complete ETL pipeline with 35 tests. It is not gone,
just not checked out: `git show 57c8192:src/main/scala/playground/etl/Extract.scala`.
