# How to use the Apache Hudi plugin

This plugin reads, writes and maintains [Apache Hudi](https://hudi.apache.org/) tables from Kestra with the Hudi Java client, without a Spark or Flink cluster.

## Authentication

Every task and the trigger take the same connection properties:

- `basePath` (required): table location, for example `file:///data/hudi/orders`.
- `tableName` (required): name of the Hudi table.
- `configuration` (optional, secret): map of Hadoop and Hudi settings, for example storage credentials or `hoodie.*` write configs.

`configuration` is marked as a secret and is hidden from logs. Store credentials in [secrets](https://kestra.io/docs/concepts/secret) and reference them with `{{ secret('NAME') }}`. To share the same connection across flows, use [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

Only the file systems present on the plugin classpath can be used. Local (`file://`) and HDFS paths work out of the box; `s3a://`, GCS and Azure paths need the matching Hadoop connector, which is not bundled yet.

## Tasks

### Write

`Upsert` and `Insert` share these properties:

- `from` (required): inline list of records, or a `kestra://` URI of a file with one JSON record per line.
- `recordKeyField` (required): field used as the Hudi record key. A record without this field fails the task.
- `partitionPathField`: field used as the partition path, empty for a non-partitioned table.
- `schema`: Avro schema as a JSON string. When omitted, it is inferred from the records.
- `precombineField`: required for `Upsert`, optional for `Insert`.

Both tasks create the table (copy on write) when `basePath` has none, and fail when Hudi reports a write error. Outputs: `recordCount`, `commitInstantTime`, `affectedPartitions`.

`Delete` removes records by key. `from` accepts a list of keys, a list of records or a `kestra://` URI, and `recordKeyField` is required. Its `recordCount` only counts records that were actually deleted.

### Read

`Commits` lists the completed commits of the active timeline with `instantTime`, `action`, `state` and `totalRecords`. Use `fetchType` (`FETCH`, `FETCH_ONE` or `STORE`) to choose how rows are returned.

### Maintenance

`Compact`, `Cluster` and `Clean` run the matching Hudi table service. They never create a table: when `basePath` has no Hudi table, the task fails.

- `Compact` only does work on merge on read tables and returns a null `instantTime` when nothing is scheduled.
- `Cluster` returns a null `instantTime` when no clustering plan is scheduled.
- `Clean` returns `instantTime` and `filesCleaned`.

Killing a `Compact` or `Cluster` execution rolls back the instant in progress.

## Triggers

`Trigger` polls the active timeline every `interval` (default `PT5M`) and starts an execution when a commit newer than the last seen one appears. The last seen commit is stored in the namespace KV store per flow, trigger and table. A missing table is skipped; any other error, such as bad credentials, fails the poll.

Outputs: `lastCommitTime`, `action`, `tableName`, `basePath`.

```yaml
id: hudi_new_commit
namespace: company.analytics

tasks:
  - id: notify
    type: io.kestra.plugin.core.log.Log
    message: "New Hudi commit on orders: {{ trigger.lastCommitTime }}"

triggers:
  - id: wait_for_commit
    type: io.kestra.plugin.hudi.Trigger
    basePath: file:///data/hudi/orders
    tableName: orders
    interval: PT2M
```
