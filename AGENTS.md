# Kestra Apache Hudi Plugin

## What

- Provides plugin components under `io.kestra.plugin.hudi` to interact with Apache Hudi transactional lakehouses.
- Includes tasks: `Upsert`, `Insert`, `Delete`, `Compact`, `Cluster`, `Clean`, `Commits`, and polling `Trigger`.

## Why

- What user problem does this solve? Teams need reliable orchestration of Apache Hudi table ingestion, compaction, cleaning, clustering, and incremental commit monitoring without deploying heavy external computing engines.
- Why would a team adopt this plugin in a workflow? It allows Kestra flows to directly perform ACID writes, deletions, and maintenance tasks on Hudi tables across cloud storages (S3, GCS, Azure) and local storage.
- What operational/business outcome does it enable? It simplifies data lakehouse management, automates table optimization, and triggers real-time downstream pipelines on new data commits.

## How

### Architecture

Single-module Kestra plugin using `hudi-java-client` 1.2.0:

- `io.kestra.plugin.hudi`

### Key Plugin Classes

- `io.kestra.plugin.hudi.AbstractHudiConnection`: Base task providing Hadoop/storage configuration and Hudi write client initialization.
- `io.kestra.plugin.hudi.Upsert`: Ingests and updates records in a Hudi table with automatic schema inference and commit tracking.
- `io.kestra.plugin.hudi.Insert`: Inserts new records into a Hudi table.
- `io.kestra.plugin.hudi.Delete`: Deletes records matching specified record keys.
- `io.kestra.plugin.hudi.Compact`: Executes scheduled compaction table services for Merge-On-Read tables with rollback on cancellation.
- `io.kestra.plugin.hudi.Cluster`: Clusters and optimizes file layout for Hudi tables.
- `io.kestra.plugin.hudi.Clean`: Removes obsolete file slices and older commit versions.
- `io.kestra.plugin.hudi.Commits`: Reads completed commits and metadata from the Hudi active timeline.
- `io.kestra.plugin.hudi.Trigger`: Polling trigger monitoring the active timeline for new completed commits using length-prefixed KV state keys.

### Project Structure

```
plugin-hudi/
├── src/main/java/io/kestra/plugin/hudi/
├── src/main/resources/doc/io.kestra.plugin.hudi.md
├── src/main/resources/icons/plugin-icon.svg
├── src/main/resources/metadata/index.yaml
├── src/test/java/io/kestra/plugin/hudi/
├── metadata/index.yaml
├── build.gradle
└── README.md
```

## Local rules

- Base the wording on the implemented packages and classes, not on template README text.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
- https://hudi.apache.org/docs/overview
