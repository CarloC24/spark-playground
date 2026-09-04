# Project: a mini lakehouse on plain Parquet

**Difficulty target: 8/10. Stack: Apache Spark 4.2, Scala 2.13, and nothing else.**

You are going to build the thing Delta Lake, Iceberg and Hudi are: a table format that turns
a directory of Parquet files into something with atomic commits, snapshots, time travel,
upserts and SQL. You will use only what is already on this project's classpath: Spark, the
Hadoop filesystem API it ships, and parquet-hadoop, which Spark depends on. No Delta jar, no
Iceberg jar, no cloud.

The point is not the format. The point is that every milestone forces you one layer beneath
the DataFrame API, into the parts of Spark the ETL pipeline in this repo never touches: how
files are listed and pruned, how Catalyst hands filters to a source, how a write is
committed, and how a DataSource V2 table plugs into the planner.

Contents

1. [What you are building](#1-what-you-are-building)
2. [Why this project, and why it is an 8](#2-why-this-project-and-why-it-is-an-8)
3. [Ground rules](#3-ground-rules)
4. [Where it lives in this repo](#4-where-it-lives-in-this-repo)
5. [Milestones](#5-milestones)
   - [M1: the log, and atomic commits](#milestone-1-the-log-and-atomic-commits)
   - [M2: reading through a FileIndex, with file skipping](#milestone-2-reading-through-a-fileindex-with-file-skipping)
   - [M3: a DataSource V2 write path, with real commits](#milestone-3-a-datasource-v2-write-path-with-real-commits)
   - [M4: upsert, copy-on-write](#milestone-4-upsert-copy-on-write)
   - [M5: time travel, checkpoints, compaction, vacuum](#milestone-5-time-travel-checkpoints-compaction-vacuum)
   - [M6: a catalog, and SQL](#milestone-6-a-catalog-and-sql)
6. [Stretch goals](#6-stretch-goals)
7. [Reading list](#7-reading-list)
8. [Order, and how long](#8-order-and-how-long)
9. [The alternative project](#9-the-alternative-project)

---

## 1. What you are building

A *lake table* is a directory:

```
data/lake/people/
├── _log/
│   ├── 00000000000000000000.json                 commit 0: metaData + first files
│   ├── 00000000000000000001.json                 commit 1: append
│   ├── 00000000000000000002.json                 commit 2: upsert (remove 1 file, add 1)
│   ├── ...
│   ├── 00000000000000000010.checkpoint.parquet   every 10 commits: the whole state
│   └── _last_checkpoint                          {"version": 10}
├── city=Lagos/part-00000-<uuid>.parquet
├── city=Oslo/part-00000-<uuid>.parquet
└── ...
```

Each commit file is a list of *actions*, one JSON object per line. The vocabulary is small:

| Action | Fields | Meaning |
|---|---|---|
| `metaData` | `id`, `schemaJson`, `partitionColumns`, `createdAt` | the table's identity and schema; in the first commit, and again only if the schema changes |
| `add` | `path`, `partitionValues`, `size`, `numRecords`, `stats`, `modificationTime` | a file that is part of the table from this commit on; `stats` holds per-column `min`, `max`, `nullCount` |
| `remove` | `path`, `deletionTimestamp` | a file that stops being part of the table at this commit |
| `commitInfo` | `operation`, `timestamp`, `readVersion` | what this commit was; used for history and for conflict detection |

A **snapshot at version N** is what you get by replaying commits 0 through N: the last
`metaData` seen, and the set of `add` paths that have no later `remove`. That set *is* the
table. Everything else in this project is a way of reading or changing that set atomically.

This is, deliberately, a simplified Delta Lake protocol. When you are unsure how something
should behave, the Delta protocol document is the reference:
https://github.com/delta-io/delta/blob/master/PROTOCOL.md

## 2. Why this project, and why it is an 8

The pipeline under `playground.etl` lives entirely in the public DataFrame API: read, select,
write. Spark is a black box that turns one into the other. This project opens the box from
the bottom:

| Layer | Where the ETL pipeline stops | Where this project goes |
|---|---|---|
| Files | `spark.read.parquet(dir)` | *you* tell Spark which files exist, per query, from a log |
| Filters | `.filter($"age" > 40)` | Catalyst hands you the expression tree and you decide which files it can skip |
| Writes | `.write.parquet(dir)` | executors write files, report to the driver, the driver commits or aborts |
| Concurrency | none | optimistic, with conflict detection and retry |
| Schema and metadata | inferred, or from a case class | stored in the log, evolved through commits |
| SQL | none | your own catalog, so `SELECT ... FROM lake.people VERSION AS OF 3` works |

What makes it hard is not the volume of code. Milestone 2 is perhaps three hundred lines.
What makes it hard is:

- The APIs are documented only by their Javadoc and by Spark's own implementations. You will
  read Spark source to make progress. That is the intended outcome, not a side effect.
- A wrong answer from your code is usually a silently wrong query result, not an exception.
  File skipping that skips one file too many returns fewer rows and nothing complains.
- Correctness under failure is the actual subject. Task retries, speculative execution, and
  two writers at once are all things the DataFrame API hides from you and this project does not.

## 3. Ground rules

- **Dependencies.** Only what `build.sbt` already has: `spark-core`, `spark-sql`, and what they
  pull in. The Postgres driver is for the ETL side and stays there.
- **Filesystem.** Every file operation goes through `org.apache.hadoop.fs.FileSystem`, never
  `java.io.File` or `java.nio.file`. Nothing then has to change to run on HDFS or S3 later, and
  the semantics you rely on (rename, create-if-absent, listing) are the ones a real deployment has.
- **Runtime.** `local[*]`, the same `--add-opens` set, the same JDK 21 pin.
- **Tests.** Every suite extends `SparkSessionTestBase` and writes into its own
  `Files.createTempDirectory`. Suites do not share tables.
- **Do not copy Delta's code.** Read its protocol, borrow its vocabulary for orientation, and
  write your own. The moments where you disagree with its design are the moments you learn
  the most.
- **Keep a `NOTES.md`.** Every time Spark surprises you, write down what you expected, what
  happened, and which Spark source file explained it. That file is the real output of the
  project.

## 4. Where it lives in this repo

```
src/main/scala/playground/lake/
  Actions.scala          M1  Add, Remove, MetaData, CommitInfo, and their JSON codec
  LakeLog.scala          M1  reading and appending the log; snapshot(version)
  Snapshot.scala         M1  the replayed state: metadata + live files
  StatsCollector.scala   M2  min / max / nullCount from Parquet footers
  DataSkipping.scala     M2  Catalyst Expression -> "can this file be skipped?"
  LakeFileIndex.scala    M2  FileIndex over a Snapshot: partition pruning + data skipping
  LakeDataSource.scala   M2  DataSourceRegister + RelationProvider, short name "lake"
  LakeTable.scala        M3  the DSv2 Table: SupportsWrite now, SupportsRead in M6
  LakeWrite.scala        M3  WriteBuilder / BatchWrite / DataWriter / commit messages
  Merge.scala            M4  upsert
  Maintenance.scala      M5  checkpoint, compact, vacuum
  LakeScan.scala         M6  the DSv2 read side: ScanBuilder / Scan / PartitionReader
  LakeCatalog.scala      M6  TableCatalog
src/main/resources/META-INF/services/
  org.apache.spark.sql.sources.DataSourceRegister      one line: playground.lake.LakeDataSource
src/test/scala/playground/lake/
```

The ETL pipeline grows a third sink, `--output-format lake`, in `Load`. `PipelineRoundTripSpec`
gets a sibling that goes JSON to lake and back, which is the same test that already holds the
Parquet and Postgres paths in place.

## 5. Milestones

Each milestone has the same shape: the goal, what to build, the Spark internals you will meet,
the first tests to write, the pitfalls, and what "done" means. Write the tests first where you
can; several of them are the only way to know the code is right.

### Milestone 1: the log, and atomic commits

**Goal.** `LakeLog.commit(actions)` appends a numbered commit file atomically, and
`LakeLog.snapshot(version)` replays the log into a `Snapshot`. Data files are still written by
`df.write.parquet` for now; after the write you find the new files and commit them as `add`
actions.

**The interface you are implementing.**

```scala
package playground.lake

sealed trait Action
case class MetaData(id: String, schemaJson: String, partitionColumns: Seq[String], createdAt: Long) extends Action
case class Add(path: String, partitionValues: Map[String, String], size: Long, numRecords: Long,
               stats: Option[String], modificationTime: Long) extends Action
case class Remove(path: String, deletionTimestamp: Long) extends Action
case class CommitInfo(operation: String, timestamp: Long, readVersion: Option[Long]) extends Action

case class Snapshot(version: Long, metadata: MetaData, files: Seq[Add]) {
  def schema: StructType
  def paths: Seq[String]          // absolute, ready for spark.read.parquet(paths: _*)
}

class LakeLog(spark: SparkSession, tableDir: Path) {
  def latestVersion: Option[Long]                 // None for a directory with no _log
  def snapshot(version: Long): Snapshot
  def snapshot(): Snapshot                         // latest
  def commit(readVersion: Option[Long], actions: Seq[Action]): Long   // returns the new version
}
```

**Build.**

1. `Actions.scala`. One JSON object per action, one action per line, so a commit file is JSON
   Lines and can be read with `spark.read.json` for debugging. Spark's own Jackson is on the
   classpath (`com.fasterxml.jackson.module.scala.DefaultScalaModule`), so use it rather than
   adding a library. A one-field wrapper object per action type (`{"add": {...}}`) is how
   Delta makes the type recoverable; do the same.
2. `LakeLog.commit`. Write the actions to a temporary file next to the log, then
   `FileSystem.rename` it to `_log/<zero-padded version>.json`. On the local filesystem and
   on HDFS, rename fails if the target exists. That failure *is* your concurrency control:
   two writers who both believe they are at version 4 cannot both create version 5. The
   alternative is `FileSystem.create(path, overwrite = false)`; write a paragraph in your
   notes on when each is safe.
3. `LakeLog.snapshot(version)`. List `_log/`, sort, replay 0 through N. Keep adds in a
   `Map[path, Add]`; a `remove` deletes the key. Keep the last `MetaData` seen.
4. A `LakeTable.write(df, mode)` helper: `df.write.mode(Append).parquet(dir)` so existing
   files are untouched, then diff the directory listing before and after to find the new
   files, then commit them. For `Overwrite`, also commit a `Remove` for every file in the
   current snapshot. This helper is a stopgap that M3 replaces, and it has a hole you should
   be able to name by the end of the milestone: a retried task can leave a file on disk that
   is in neither the "before" nor the committed set. It is harmless, because the log and not
   the directory defines the table, and M5's vacuum removes it.

**What you meet.** `org.apache.hadoop.fs.{FileSystem, Path, FileStatus}`, obtained via
`path.getFileSystem(spark.sessionState.newHadoopConf())`. Why a `_SUCCESS` marker is not a
transaction. Why "list the directory" is the wrong definition of a table. JSON Lines.

**First tests.**

- Two appends produce versions 0 and 1. `snapshot(1).files` holds both file sets;
  `snapshot(0).files` only the first.
- Overwrite commits a `remove` for every prior file and an `add` for each new one. The old
  files still exist on disk but are absent from the snapshot.
- Committing when the target version already exists throws and leaves `_log/` unchanged.
  Simulate it by writing the file yourself first.
- `spark.read.parquet(snapshot.paths: _*)` returns exactly the rows that were written.
- The five people, JSON to lake to `spark.read.parquet`, round-trip equal.

**Pitfalls.** Zero-pad version numbers to twenty digits so lexical order is numeric order.
`FileSystem.rename` has different semantics per filesystem: on S3A it overwrites silently.
Note it; do not solve it. Store paths in the log relative to the table directory, so the
table can be moved.

**Done when.** The round-trip test and the commit-collision test pass.

### Milestone 2: reading through a FileIndex, with file skipping

**Goal.** `spark.read.format("lake").load("data/lake/people").filter($"age" > 40)` plans a scan
that touches only the files whose stats admit `age > 40`, and does so through Spark's real
vectorized Parquet reader.

This is the milestone that changes how you see Spark. Every file-based DataFrame is a
`HadoopFsRelation(location: FileIndex, partitionSchema, dataSchema, bucketSpec, fileFormat, options)`.
The `FileIndex` is the object Spark asks "which files, given these filters?". Replace it with
one that answers from the log, and everything downstream (column pruning, the vectorized
reader, partition columns, the `_metadata` column) keeps working unchanged. This is exactly
how Delta's `TahoeFileIndex` works.

**The interface you are implementing.**

```scala
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitionDirectory}
import org.apache.spark.sql.catalyst.expressions.Expression

class LakeFileIndex(spark: SparkSession, snapshot: Snapshot) extends FileIndex {
  def rootPaths: Seq[Path]
  def partitionSchema: StructType
  def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory]
  def inputFiles: Array[String]
  def refresh(): Unit
  def sizeInBytes: Long
}

object DataSkipping {
  /** True only when no row in a file with these stats can satisfy the filter. */
  def canSkip(filter: Expression, stats: FileStats): Boolean
}
```

**Build.**

1. `StatsCollector`. After a file is written, open its footer:
   `ParquetFileReader.open(HadoopInputFile.fromPath(path, conf)).getFooter`, then walk
   `getBlocks` (row groups), each block's `getColumns`, and each column chunk's
   `getStatistics`. Fold min, max and null count across row groups into one entry per
   column and store it in `Add.stats` as JSON. Skip nested columns (`hobbies`) for now.
2. `LakeFileIndex`. `rootPaths` is the table directory. `partitionSchema` comes from the
   log's metadata. `listFiles` returns one `PartitionDirectory` per distinct partition
   value, holding the `FileStatus` of each surviving file. Partition pruning first: evaluate
   `partitionFilters` against each file's `partitionValues` (Spark's own
   `PartitioningAwareFileIndex` shows how to bind a Catalyst predicate to a row and evaluate
   it). Then data skipping on `dataFilters`. `sizeInBytes` is the sum of `Add.size`; it is
   the number the planner uses to decide on broadcast joins, so it has to be honest.
3. `DataSkipping.canSkip`. Translate the Catalyst tree. `EqualTo(attr, lit)` skips when the
   literal is below min or above max. `LessThan`, `GreaterThan` and their `OrEqual`
   variants likewise. `IsNull` skips when `nullCount == 0`; `IsNotNull` skips when
   `nullCount == numRecords`. `And` skips if either side skips. `Or` skips only if both
   sides skip. Anything you do not recognize: do not skip. Read that last rule twice.
   Skipping must be conservative; a false skip is a silently wrong result.
4. `LakeDataSource extends DataSourceRegister with RelationProvider`. `shortName` is
   `"lake"`. `createRelation` builds a `HadoopFsRelation` with your index and a fresh
   `ParquetFileFormat`. Honour a `versionAsOf` option. Register the class in
   `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`.

**What you meet.** `FileIndex`, `PartitionDirectory`, `HadoopFsRelation`, `ParquetFileFormat`.
Catalyst `Expression`, `AttributeReference`, `Literal`, `And`, `Or`, `Not`. `FileSourceStrategy`,
which is where your `listFiles` gets called and where filters get split into partition and
data filters. Parquet footers, row groups, column chunk statistics. Three-valued logic.

**First tests.**

- Write people partitioned by `city`. `filter($"city" === "Oslo")` results in `listFiles`
  being called with one partition filter, and `df.inputFiles` lists only Oslo's file.
  Assert on the files, not only the rows.
- Write two files, one with ages 20 to 30 and one with 40 to 50. `filter($"age" > 35)` reads
  one file.
- `filter($"age" > 35 || $"city" === "Lagos")` reads both files, because `Or` skips only when
  both sides skip.
- `filter($"age".isNull)` reads no files when every file has `nullCount == 0`.
- `option("versionAsOf", 0)` after an append returns only the first batch.
- The property test: for a few hundred random simple filters over random small tables, the
  rows from the lake reader equal the rows from `spark.read.parquet(allFiles).filter(...)`.
  This test earns its keep for the rest of the project. Write it early.

**Pitfalls.**

- The expressions in `dataFilters` are unbound; match attributes by `AttributeReference.name`.
- Binary and string min/max in footers can be truncated by the writer. Treat them as bounds,
  never as exact values.
- `Not` inverts three-valued logic. `Not(EqualTo(a, 5))` on a file with `min = max = 5` can be
  skipped; on a file with `min = 5, max = 6` it cannot. Simplest correct rule: never skip on
  `Not` until you have worked the cases through and have tests for each.
- `HadoopFsRelation` and `LogicalRelation` are public, but `Dataset.ofRows` is
  `private[sql]`. Going through `RelationProvider` means you never need it. If you find you
  genuinely need an internal, put that one file in a package under `org.apache.spark.sql` and
  say why in a comment. Do not make a habit of it.

**Done when.** The property test passes, and `explain(true)` on a filtered read shows a
`FileScan parquet` whose file count matches what the skipping should allow.

### Milestone 3: a DataSource V2 write path, with real commits

**Goal.** `df.write.format("lake").mode("append" | "overwrite").save(path)` writes files from
executors and commits them from the driver, as one commit, with conflict detection.

M1's write was "write, then look at the directory". That is not how a commit protocol works.
Executors write files and report *what they wrote* to the driver; the driver commits the
whole set or nothing. Spark's DataSource V2 write API has exactly this shape, and Spark's own
Parquet writer (`FileFormatWriter` plus `HadoopMapReduceCommitProtocol`) does the same thing
internally.

**The interfaces you are implementing** (all from `org.apache.spark.sql.connector`):

```
catalog.TableProvider           getTable(schema, partitioning, properties) -> LakeTable
catalog.Table                   name, schema, partitioning, capabilities
catalog.SupportsWrite           newWriteBuilder(LogicalWriteInfo) -> LakeWriteBuilder
write.WriteBuilder              build() -> Write
write.SupportsTruncate          truncate(): overwrite mode arrives as "truncate then write"
write.Write                     toBatch -> BatchWrite
write.BatchWrite                createBatchWriterFactory(PhysicalWriteInfo); commit(messages); abort(messages)
write.DataWriterFactory         createWriter(partitionId, taskId) -> DataWriter   [serialized to executors]
write.DataWriter[InternalRow]   write(row); commit() -> WriterCommitMessage; abort(); close()
write.WriterCommitMessage       your case class: path, partitionValues, size, numRecords, stats
```

**Build.**

1. `LakeTable` with `capabilities = {BATCH_WRITE, TRUNCATE}`. Not `BATCH_READ` yet; see the
   first pitfall for why that is the whole trick.
2. `LakeWriteBuilder`. Record whether `truncate()` was called; that is how `mode("overwrite")`
   reaches you.
3. The writer factory and writer. The factory is shipped to executors, so it and everything it
   references must be `Serializable` and must not capture the `SparkSession`. The writer opens
   a Parquet writer on a uniquely named file, writes rows, and on `commit()` returns a message
   carrying the path, size, record count, partition values, and stats. Compute stats *in the
   writer* as rows go by, not in a second pass. `abort()` deletes the file.
4. `BatchWrite.commit(messages)` on the driver: turn messages into `Add` actions, add
   `Remove`s for overwrite, then `LakeLog.commit` inside a retry loop. Read the latest
   version; if it moved past your read version, check whether the intervening commits
   conflict; retry or fail. First conflict rule: an append never conflicts with an append;
   anything else conflicts with anything. M4 tightens it.
5. `LakeDataSource` now also `extends TableProvider`, so `format("lake")` reaches the V2 path.

**What you meet.** The two-phase commit shape behind every Spark write. `InternalRow`, and
why it is not a `Row`. `TableCapability`. `LogicalWriteInfo.queryId`. Task retries and
speculative execution: two attempts of the same task can both `commit()`, Spark uses only
one message, and the other attempt's file is an orphan you must tolerate.

**First tests.**

- Append: the version increments by one, one `Add` per commit message, row count matches.
- Overwrite: every old file removed and every new one added, in a single commit.
- Force a failure on the first attempt of one task (a writer that throws when
  `TaskContext.get.attemptNumber == 0 && partitionId == 0`; set `spark.task.maxFailures`
  above 1 in the test session) and check the table has exactly the right rows and the log
  has exactly one new commit.
- Two `BatchWrite`s prepared against version N, committed one after the other: the second
  detects the moved version, retries, and lands as N+2 for appends, or fails for
  overwrite-after-overwrite.
- The stats in each `Add` equal what `StatsCollector` reads back from the file's footer.

**Pitfalls.**

- A V2 table's read side is a `ScanBuilder`, not a `FileIndex`, and the two do not mix. The
  way through: when a `TableProvider`'s table does *not* declare `BATCH_READ`,
  `DataFrameReader.load` falls back to the V1 `RelationProvider` with the same short name,
  while `DataFrameWriter.save` uses the V2 write path because the table declares
  `BATCH_WRITE`. So M2 reads and M3 writes coexist on one class until M6 moves reads to V2.
  Read `DataFrameReader.load` and `DataSourceV2Utils.loadV2Source` to see the fallback.
- `partitionValues` must come from the row, not from the task. A task can hold rows for
  several partitions unless the frame was repartitioned first; open one file per partition
  value per task.
- Do not let a `WriterCommitMessage` reference a `FileStatus` or anything else Hadoop; keep
  it plain case-class data.

**Done when.** The task-retry test and the concurrent-append test pass.

### Milestone 4: upsert, copy-on-write

**Goal.** `Merge.upsert(table, source: DataFrame, on = "id")`: rows in `source` replace matching
rows in the table, unmatched source rows are inserted, all in one commit, rewriting only the
files that contain a match.

**Build.**

1. Pin a snapshot version at the start. Every read below is against that version.
2. Read the table with its `_metadata.file_path` column. Spark exposes `_metadata` on every
   file-based scan, including yours from M2.
3. Touched files: `table.join(source, "id").select($"_metadata.file_path").distinct.collect()`.
   Broadcast the source when it is small, and look at the plan to confirm it happened.
4. Survivors: `spark.read.parquet(touched: _*).join(source, Seq("id"), "left_anti")`, the rows in
   touched files that are *not* being replaced.
5. Write `survivors.unionByName(source)` as new files through M3's writer, then commit
   `Remove(touched) ++ Add(newFiles)` with `readVersion` set to the pinned version.
6. Tighten conflict detection: a concurrent commit conflicts if it removed any file you
   touched, or added a file to a partition you read. `CommitInfo.readVersion` and the
   partition values in `Add` are what make this decidable.

**What you meet.** `_metadata` columns. Join strategy selection and `broadcast`. `left_anti`.
The cost model of copy-on-write: one changed row rewrites its whole file, which is why file
size matters and why M5 exists. Optimistic concurrency with a real conflict rule.

**First tests.**

- Upsert 2 changed rows and 1 new row into a 5-row, 2-file table: one file rewritten, the
  other untouched (assert on the log), 6 rows after, changed rows show the new values.
- A source with two rows for the same key: decide on a policy (fail is the sane default),
  then test it.
- An empty source produces no commit at all; the version is unchanged.
- Concurrency, with the same simulated-writer trick as M3: an append to an unrelated
  partition between read and commit, and the upsert succeeds on retry; a removal of a
  touched file, and the upsert fails with a conflict error.

**Pitfalls.** The touched-files join and the survivors anti-join must see the same snapshot;
that is what step 1 is for. Cache the source if it is anything but trivial, because it is read
at least twice.

**Done when.** The four tests pass and the plan for a small source shows a broadcast join.

### Milestone 5: time travel, checkpoints, compaction, vacuum

**Goal.** Reads at any version stay fast after thousands of commits; small files get merged;
unreferenced files get deleted, safely.

**Build.**

1. **Checkpoints.** Every ten commits, write the full snapshot (all live `Add`s plus the
   `MetaData`) as `_log/<version>.checkpoint.parquet`, via `spark.createDataset(...)`, and
   write `_last_checkpoint` holding the version. `snapshot(N)` now reads the newest checkpoint
   at or below N, then only the JSON commits after it.
2. **Time travel by timestamp.** `CommitInfo.timestamp` per commit; `versionAt(ts)` is the
   greatest version whose timestamp is at or before `ts`. Expose `timestampAsOf` beside
   `versionAsOf`.
3. **Compaction.** Per partition, bin-pack the files smaller than a target size (use a few
   megabytes in tests) into groups near the target, rewrite each group as one file through
   M3's writer, and commit `Remove(group) ++ Add(one)`. Compaction changes no rows, so it never
   conflicts with an append; it does conflict with an upsert that removed one of its inputs.
4. **Vacuum.** Delete every file under the table directory that no snapshot inside the
   retention window references. Default the window to seven days; make it overridable for
   tests. Two steps: a dry run that lists, then the delete. Vacuum is what turns M3's orphan
   files from a wart into a non-issue.

**What you meet.** Log compaction as a pattern. Why Delta writes checkpoints as Parquet and
commits as JSON. The small-files problem and its cost in both `listFiles` and task
scheduling. The tension between time travel and vacuum.

**First tests.**

- 25 appends produce 2 checkpoints. `snapshot(25)` equals a naive replay of all 26 JSON files.
  The snapshot read opens only the JSON files after the last checkpoint (count the opens with
  a wrapped `FileSystem`, or assert on timing at a much larger commit count).
- `versionAsOf(3)` and `timestampAsOf(the timestamp of commit 3)` return the same rows.
- Compacting ten tiny files in one partition yields one file, the same rows, one commit.
- After an overwrite, vacuum deletes the removed files and none of the live ones. With a
  retention window longer than the table's age, it deletes nothing.

**Pitfalls.** The checkpoint schema must carry every action type's fields, all nullable, in one
struct. Clock skew between writers makes commit timestamps non-monotonic; the Delta rule is to
bump each commit's timestamp to at least the previous one's.

**Done when.** Reading version 1000 of a 1000-commit table takes about the same time as
reading version 10.

### Milestone 6: a catalog, and SQL

**Goal.**

```sql
CREATE TABLE lake.people (id BIGINT, name STRING, email STRING, age INT, city STRING)
  USING lake PARTITIONED BY (city);
INSERT INTO lake.people SELECT * FROM json_people;
SELECT * FROM lake.people VERSION AS OF 2 WHERE age > 40;
```

**Build.**

1. `LakeCatalog extends TableCatalog`. Tables map to directories under a warehouse root given
   as a catalog option. `loadTable(ident)`, the `loadTable(ident, version)` and
   `loadTable(ident, timestamp)` overloads (that is what `VERSION AS OF` calls), `createTable`,
   `dropTable`, `listTables`. Add `SupportsNamespaces` if you want `SHOW TABLES`.
2. Register it: `spark.sql.catalog.lake = playground.lake.LakeCatalog` and
   `spark.sql.catalog.lake.warehouse = data/lake`.
3. Reads now have to be V2: `LakeTable` gains `BATCH_READ` and `newScanBuilder`. The builder
   implements `SupportsPushDownFilters` and `SupportsPushDownRequiredColumns`. Translate the
   pushed `Filter`s into your M2 skipping predicate, and return *all* of them as post-scan
   filters so Spark re-evaluates them on the rows. `Scan.toBatch` yields one `InputPartition`
   per file (or per bundle of small files) and a `PartitionReaderFactory` whose reader
   decodes Parquet into `InternalRow`. The simplest correct decoder is parquet-hadoop's
   `ParquetReader` with a `GroupReadSupport`, converted by hand for the types you support. The
   fast one is a copy of what Spark's `ParquetPartitionReaderFactory` does.
4. History: a `LakeTable.history: DataFrame` helper over the `CommitInfo`s. SQL syntax for it
   would need a parser extension, which is optional.

**What you meet.** `CatalogPlugin`, `TableCatalog`, `Identifier`. How `spark.sql` resolves
`lake.people` (the analyzer's relation resolution consults your catalog). DSv2 push-down in
`V2ScanRelationPushDown`. The difference between a filter you *evaluate* and one you merely
*prune with*. `InputPartition` granularity and its effect on task count.

**First tests.**

- CREATE, INSERT, SELECT through SQL only; the log shows the expected commits.
- `VERSION AS OF 0` after two inserts returns only the first batch.
- `EXPLAIN` on a filtered SELECT shows your scan with the pushed filters listed, and the
  number of input partitions matches what M2's skipping allows.
- Reads through the catalog and through `spark.read.format("lake")` on the same directory
  agree, row for row.
- The M2 property test, run through SQL against the catalog.

**Pitfalls.** Stats-based skipping is pruning, not evaluation; return every pushed filter as a
post-scan filter unless your reader applies it exactly. Column pruning must honour the
requested column order. A `Table` instance is created per query; do not cache a snapshot in
it beyond that query.

**Done when.** The property test passes through SQL.

## 6. Stretch goals

Each of these pushes toward 9/10.

**7. A streaming sink with exactly-once.** `writeStream.format("lake")` on a `MemoryStream` or a
file source. Add `STREAMING_WRITE` to the capabilities and implement `StreamingWrite`. Its
`commit(epochId, messages)` must be idempotent: record the query id and epoch in
`CommitInfo`, and on restart skip an epoch the log already holds. Test it by stopping the
query between the executor writes and the driver commit, then restarting from the
checkpoint. In Spark 4.2 `MemoryStream` is `org.apache.spark.sql.execution.streaming.runtime.MemoryStream`;
it is a test utility rather than public API, which is fine for a test.

**8. MERGE INTO in SQL, planned by Spark.** Implement `SupportsRowLevelOperations` on
`LakeTable`. `newRowLevelOperationBuilder` returns a `RowLevelOperation` for DELETE, UPDATE and
MERGE in copy-on-write mode. You provide a scan that also exposes the file identity through
`requiredMetadataAttributes`, and a write that replaces the files Spark reports it fully read.
Spark plans the join, the survivors and the rewrite; you supply M4's commit. Spark's
`InMemoryRowLevelOperationTable` test class is the map.

**9. Clustering.** During compaction, sort rows by a Z-curve over two columns and show, with
M2's stats, that filters on both columns skip more files than before.

## 7. Reading list

Spark source, `apache/spark` at tag `v4.2.0`. Paths under `sql/`.

| File | Why | Milestone |
|---|---|---|
| `core/.../execution/datasources/FileIndex.scala`, `PartitioningAwareFileIndex.scala`, `InMemoryFileIndex.scala` | the trait you implement, and Spark's own implementations of it | M2 |
| `core/.../execution/datasources/HadoopFsRelation.scala`, `FileSourceStrategy.scala` | where `listFiles` is called and filters are split | M2 |
| `core/.../execution/datasources/DataSourceStrategy.scala` | Catalyst expressions to source filters | M2, M6 |
| `core/.../execution/datasources/parquet/ParquetFileFormat.scala` | the reader you reuse | M2 |
| `core/.../execution/datasources/FileFormatWriter.scala` and `core/src/main/scala/org/apache/spark/internal/io/HadoopMapReduceCommitProtocol.scala` (in the `core` module) | the commit protocol Spark itself uses | M3 |
| `core/.../DataFrameReader.scala` and `core/.../execution/datasources/v2/DataSourceV2Utils.scala` | the V2-to-V1 fallback M3 relies on | M3 |
| `catalyst/src/main/java/org/apache/spark/sql/connector/**` | every DSv2 interface, with its Javadoc | M3, M6, stretch |
| `catalyst/src/test/.../connector/catalog/InMemoryTable.scala`, `InMemoryTableCatalog.scala`, `InMemoryRowLevelOperationTable.scala` | the reference implementations | M3, M6, 8 |
| `core/src/test/.../connector/SimpleWritableDataSource.scala` | a complete, minimal DSv2 file source | M3 |
| `core/.../execution/datasources/v2/V2ScanRelationPushDown.scala` | how push-down reaches your scan builder | M6 |
| `core/.../execution/datasources/v2/parquet/ParquetPartitionReaderFactory.scala` | the fast reader to model | M6 |

Not Spark:

- The Delta Lake protocol, https://github.com/delta-io/delta/blob/master/PROTOCOL.md. The
  whole project.
- The Parquet format, https://github.com/apache/parquet-format, especially column chunk
  statistics and the column index. M2.
- "Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores", VLDB 2020. The
  paper behind the design, and short.

## 8. Order, and how long

M1, M2, M3, M4, M5, M6, then stretch. Do not skip ahead; each one's tests are the safety net
for the next.

- M1 and M5 are a weekend each.
- M2, M3 and M4 are the heart of the project, and each will take longer than you estimate,
  because most of the time goes to reading Spark rather than writing Scala. That is the
  point.
- M6 is mechanical once M3 exists, except for the Parquet reader, which is its own small
  project.

Keep the `NOTES.md` from the ground rules. A finished `NOTES.md` is worth more than the code.

## 9. The alternative project

If streaming interests you more than storage: a stateful sessionization engine on Structured
Streaming. Read click events from a file or rate source, assign them to sessions with
`transformWithState` and a custom `StatefulProcessor` (new in Spark 4), close sessions with
event-time watermarks and a timeout, handle late data, write closed sessions to partitioned
Parquet, and prove exactly-once by killing and restarting the query mid-batch. It sits
around 7/10 and is narrower than the lakehouse, which is why the lakehouse is the
recommendation. The two meet at stretch goal 7, where the lakehouse becomes the streaming
sink.
