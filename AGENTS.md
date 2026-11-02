# Kestra Apache Hudi Plugin

## What

- Provides plugin components under `io.kestra.plugin.hudi` to interact with Apache Hudi transactional lakehouses.
- Includes tasks: `Upsert`, `Insert`, `Delete`, `Compact`, `Cluster`, `Clean`, `Commits`, and polling `Trigger`.

## Why

- What user problem does this solve? Teams need reliable orchestration of Apache Hudi table ingestion, compaction, cleaning, clustering, and incremental commit monitoring without deploying heavy external computing engines.
- Why would a team adopt this plugin in a workflow? It allows Kestra flows to directly perform ACID writes, deletions, and maintenance tasks on Hudi tables on local paths (inside the task working directory) and on HDFS out of the box. `s3a`, `gs` and `abfs` paths are accepted but need the matching Hadoop connector on the worker classpath, which is not bundled.
- What operational/business outcome does it enable? It simplifies data lakehouse management, automates table optimization, and triggers real-time downstream pipelines on new data commits.

## How

### Architecture

Single-module Kestra plugin using `hudi-java-client` 1.2.0 and Hadoop 3.4.3 (runs on Java 21 and 25):

- `io.kestra.plugin.hudi`

### Key Plugin Classes

- `io.kestra.plugin.hudi.AbstractHudiConnection`: Base task providing `basePath` resolution, the `configuration` allow-list, Hadoop configuration and Hudi write client initialization.
- `io.kestra.plugin.hudi.AbstractHudiWrite`: Base of `Upsert` and `Insert`, holding the shared properties (record key, precombine, partition, schema, `from`) and the write and commit logic.
- `io.kestra.plugin.hudi.AbstractHudiMaintenance`: Base of `Compact`, `Cluster` and `Clean`, with the `kill()` flag. `kill()` only sets the flag; `Cluster` rolls back its own instant when it sees it, `Compact` leaves a requested plan to be resumed on the next run.
- `io.kestra.plugin.hudi.HudiRecordUtils`: Parses `from` input, infers the Avro schema and builds Hudi records and delete keys.
- `io.kestra.plugin.hudi.Upsert`: Ingests and updates records in a Hudi table with automatic schema inference and commit tracking.
- `io.kestra.plugin.hudi.Insert`: Inserts new records into a Hudi table.
- `io.kestra.plugin.hudi.Delete`: Deletes records matching specified record keys.
- `io.kestra.plugin.hudi.Compact`: Schedules and runs compaction on Merge-On-Read tables (tables created by this plugin are Copy-On-Write, so there is nothing to compact there).
- `io.kestra.plugin.hudi.Cluster`: Schedules and runs clustering to merge small files.
- `io.kestra.plugin.hudi.Clean`: Removes obsolete file slices and older commit versions.
- `io.kestra.plugin.hudi.Commits`: Reads completed commits and metadata from the Hudi active timeline.
- `io.kestra.plugin.hudi.Trigger`: Polling trigger on a remote table path (a trigger has no shared working directory) that watches the active timeline for new completed commits, with a watermark stored in the namespace KV store.

### Project Structure

```
plugin-hudi/
├── src/main/java/io/kestra/plugin/hudi/
├── src/main/resources/doc/io.kestra.plugin.hudi.md
├── src/main/resources/icons/plugin-icon.svg
├── src/main/resources/metadata/index.yaml
├── src/test/java/io/kestra/plugin/hudi/
├── src/test/resources/core-site.xml   (test only: serves s3a from local disk)
├── build.gradle
└── README.md
```

## Commands

```bash
./gradlew --no-daemon test         # unit tests, JDK 21 or 25
./gradlew --no-daemon check        # tests and plugin doc lint
```

## Local rules

- Base the wording on the implemented packages and classes, not on template README text.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
- https://hudi.apache.org/docs/overview
