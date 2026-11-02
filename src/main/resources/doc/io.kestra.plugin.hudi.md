# How to use the Apache Hudi plugin

This plugin reads, writes and maintains [Apache Hudi](https://hudi.apache.org/) tables from Kestra with the Hudi Java client, without a Spark or Flink cluster.

## Authentication

Every task and the trigger take the same connection properties:

- `basePath` (required): table location, for example `hdfs://namenode:8020/data/hudi/orders`. Allowed remote schemes: `hdfs`, `s3a`, `gs`, `abfs`, `abfss`, `wasb`, `wasbs`. A local path (relative, absolute or `file://`) must be inside the working directory and is resolved against it.
- `tableName` (required): name of the Hudi table.
- `configuration` (optional, secret): map of Hadoop and Hudi settings, for example storage credentials or `hoodie.*` write configs. Allowed configuration keys start with `hoodie.`, `fs.s3a.`, `fs.gs.`, `fs.azure.` or `fs.abfs.`. Within `hoodie.`, only write and table tuning keys are accepted: record key, precombine and partition path fields, table name and type, index type, insert, upsert, bulk insert and delete parallelism, and the `hoodie.cleaner.*`, `hoodie.clean.*`, `hoodie.compact.*`, `hoodie.clustering.*`, `hoodie.archive.*`, `hoodie.keep.*`, `hoodie.parquet.*`, `hoodie.logfile.*`, `hoodie.copyonwrite.*` and `hoodie.index.bloom.*` groups. Other `hoodie.` keys are rejected, as is any key ending with `.path`, `.dir` or `.pattern`, or containing `.class`, `.impl`, `.factory` or `classname`. Within the `fs.*` prefixes, keys ending with `.impl`, `.class`, `.classname` or `.factory`, and keys containing `provider`, `signers`, `interceptors`, `binding`, `keyfile` or `keytab`, are not allowed, except the credential and endpoint keys `access.key`, `secret.key`, `session.token`, `endpoint`, `path.style.access` and `region`. `fs.defaultFS` and `fs.viewfs.*` are not allowed, the scheme comes from `basePath`. Values must be literal: variable expansion is not applied and values containing `${` are rejected.

`configuration` is marked as a secret and is hidden from logs. Store credentials in [secrets](https://kestra.io/docs/concepts/secret) and reference them with `{{ secret('NAME') }}`. To share the same connection across flows, use [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

Only the file systems present on the plugin classpath can be used. HDFS paths and local paths work out of the box; `s3a://`, `gs://` and `abfs://` paths only work when the matching Hadoop connector (`hadoop-aws`, `gcs-connector`, `hadoop-azure`) is added to the worker classpath, it is not bundled.

A local table lives and dies with the working directory of the task. To use one table from several tasks, wrap them in `io.kestra.plugin.core.flow.WorkingDirectory` so they share the directory.

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

`Commits` lists the completed commits of the active timeline with `instantTime`, `action`, `state` and `totalRecords`. `totalRecords` is the number of records written by that commit, including records rewritten by copy on write file rewrites, not the size of the table after the commit (for example a delete commit can show 0). Use `fetchType` (`FETCH`, `FETCH_ONE` or `STORE`) to choose how rows are returned.

### Maintenance

`Compact`, `Cluster` and `Clean` run the matching Hudi table service. They never create a table: when `basePath` has no Hudi table, the task fails.

- `Compact` only does work on merge on read tables created outside this plugin, since this plugin creates copy on write tables only. It returns a null `instantTime` when nothing is scheduled.
- `Cluster` returns a null `instantTime` when no clustering plan is scheduled.
- `Clean` returns `instantTime` and `filesCleaned`.

Killing a `Compact` or `Cluster` execution does not roll back the instant in progress. A crashed or killed `Compact` can leave an inflight compaction instant on the timeline. Because it may be running in another engine, `Compact` never rolls it back or executes it, and fails on later runs until the instant is completed or rolled back. To clear it, make sure nothing is still running it, then roll it back with Hudi tooling, for example `compaction unschedule` or `commit rollback` in `hudi-cli`. A plan left in the requested state is executed by the next `Compact` run.

### Delete

`Delete` locates a record by its key and partition path. For a partitioned table, set `partitionPathField` and make `from` contain records with both the record key field and the partition path field; bare keys fail the task with an error naming `from` and `partitionPathField`. Bare keys work on non-partitioned tables only, so leave `partitionPathField` unset for those.

## Triggers

`Trigger` needs a remote `basePath` (one of the allowed remote schemes), because a trigger has no shared working directory: it is new and empty on every check, so a local table could never be found. `Trigger` polls the active timeline every `interval` (default `PT5M`) and starts an execution when a commit newer than the last seen one appears. The last seen commit is stored in the namespace KV store per flow, trigger and table. A missing table is skipped; any other error, such as bad credentials, fails the poll.

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
    basePath: hdfs://namenode:8020/data/hudi/orders
    tableName: orders
    interval: PT2M
```
