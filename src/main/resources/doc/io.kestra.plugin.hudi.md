# Apache Hudi Plugin

The Apache Hudi plugin allows Kestra workflows to manage and ingest data into Apache Hudi tables (Hadoop Upsert Delete and Incremental).

Apache Hudi is a transactional data lakehouse platform that brings database and data warehouse core capabilities directly to data lake storage (S3, GCS, Azure Blob, HDFS, or local storage).

---

## Architecture Overview

The plugin leverages the official Apache Hudi Java Client (`org.apache.hudi:hudi-java-client`) to perform ACID transactions, incremental processing, and table maintenance without requiring external Spark or Flink clusters.

### Key Concepts

1. **Table Types**:
   - **Copy on Write (COW)**: Data is stored exclusively in columnar Parquet files. Every update produces new versions of the modified files. Ideal for read-heavy analytical workloads.
   - **Merge on Read (MOR)**: Updates are written quickly to delta log files (Avro) and merged on read or compacted into new Parquet files asynchronously. Ideal for write-heavy streaming workloads.

2. **Timeline & Instants**:
   - All actions on a Hudi table are recorded as instants on a timeline (`.hoodie/` directory).
   - Instants transition through states: `REQUESTED` -> `INFLIGHT` -> `COMPLETED`.
   - Actions include `commits`, `deltacommit`, `clean`, `compaction`, and `replacecommit` (clustering).

3. **Storage Configuration**:
   - Connection properties (like AWS S3 credentials, Hadoop configurations, or Hudi write configs) are configured under `configuration`.

---

## Tasks Included in this Plugin

| Task | Type | Description |
|------|------|-------------|
| `io.kestra.plugin.hudi.Upsert` | Task | Upserts records into an Apache Hudi table (inserts new records or updates existing keys). |
| `io.kestra.plugin.hudi.Insert` | Task | Inserts new records into an Apache Hudi table. |
| `io.kestra.plugin.hudi.Delete` | Task | Deletes records matching specific record keys. |
| `io.kestra.plugin.hudi.Compact` | Task | Executes scheduled compactions for Merge-On-Read tables. |
| `io.kestra.plugin.hudi.Cluster` | Task | Clusters and re-writes data files to optimize query performance and layout. |
| `io.kestra.plugin.hudi.Clean` | Task | Cleans older, obsolete file slices and timeline metadata. |
| `io.kestra.plugin.hudi.Commits` | Task | Reads and retrieves commit metadata from the Hudi active timeline. |
| `io.kestra.plugin.hudi.Trigger` | Trigger | Polls the active timeline and starts an execution whenever new commits appear. |

---

## How-To Guides & Workflow Examples

### 1. Ingesting & Upserting Records

```yaml
id: hudi_upsert_pipeline
namespace: company.analytics

tasks:
  - id: ingest_orders
    type: io.kestra.plugin.hudi.Upsert
    basePath: s3a://my-lakehouse/tables/orders
    tableName: orders
    recordKeyField: order_id
    partitionPathField: region
    precombineField: updated_at
    configuration:
      fs.s3a.access.key: "{{ secret('AWS_ACCESS_KEY_ID') }}"
      fs.s3a.secret.key: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
      fs.s3a.endpoint: "s3.amazonaws.com"
    from:
      - order_id: 1001
        region: us-east
        updated_at: 1700000000
        customer: "Alice"
        total: 129.99
      - order_id: 1002
        region: eu-west
        updated_at: 1700000000
        customer: "Bob"
        total: 49.50
```

### 2. Deleting Records

```yaml
id: hudi_delete_flow
namespace: company.analytics

tasks:
  - id: purge_orders
    type: io.kestra.plugin.hudi.Delete
    basePath: s3a://my-lakehouse/tables/orders
    tableName: orders
    recordKeyField: order_id
    from:
      - order_id: 1001
```

### 3. Table Maintenance (Compaction & Cleaning)

```yaml
id: hudi_maintenance_schedule
namespace: company.analytics

tasks:
  - id: run_compaction
    type: io.kestra.plugin.hudi.Compact
    basePath: s3a://my-lakehouse/tables/orders
    tableName: orders

  - id: clean_old_versions
    type: io.kestra.plugin.hudi.Clean
    basePath: s3a://my-lakehouse/tables/orders
    tableName: orders
```

### 4. Triggering Workflows on New Commits

```yaml
id: downstream_hudi_sync
namespace: company.analytics

tasks:
  - id: notify
    type: io.kestra.plugin.core.log.Log
    message: "New Hudi commit detected on orders: {{ trigger.lastCommitTime }}"

triggers:
  - id: wait_for_commit
    type: io.kestra.plugin.hudi.Trigger
    basePath: s3a://my-lakehouse/tables/orders
    tableName: orders
    interval: PT2M
```
