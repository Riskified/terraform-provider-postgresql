---
layout: "postgresql"
page_title: "PostgreSQL: postgresql_crdb_changefeed"
sidebar_current: "docs-postgresql-resource-postgresql_crdb_changefeed"
description: |-
  Creates and manages a change-feed.
---

# postgresql_crdb_changefeed

The **`postgresql_crdb_changefeed`** resource is used to create and manage a changefeed.

> **Note:** Ensure that the following command is executed on your cluster to enable rangefeeds:
> ```sql
> SET CLUSTER SETTING kv.rangefeed.enabled = true;
> ```

## Usage

```hcl
resource "postgresql_crdb_changefeed" "my_changefeed" {
  table_list=["table1,table2"]
  avro_schema_prefix="my_avro_prefix"
  kafka_connection_name="my_kafka_connection"
  registry_connection_name="my_registry_connection"
  start_from="2025-03-19 11:00:00"
  initial_scan="no" 
```

## Argument Reference

- **`table_list`** (Required): A list of tables to include in the changefeed. The specified tables must exist in the database.

- **`avro_schema_prefix`** (Required): The prefix for the Avro schema. The changefeed will append an underscore (`_`) to the end of this prefix.
  - Example: `avro_schema_prefix = 'my_avro_prefix_'`.

- **`kafka_connection_name`** (Required): Specifies the external connection to the Kafka cluster. This connection must exist in the database.
  - This can also be generated from the `postgresql_external_connection` resource.
  - For details, refer to the [CockroachDB documentation](https://www.cockroachlabs.com/docs/v25.1/create-external-connection.html).

- **`registry_connection_name`** (Required): Specifies the external connection to the schema registry. This connection must exist in the database.
  - This can also be generated from the `postgresql_external_connection` resource.
  - For details, refer to the [CockroachDB documentation](https://www.cockroachlabs.com/docs/v25.1/create-external-connection.html).

- **`start_from`** (Optional): Defines the timestamp for the `cursor` to begin. If omitted, the changefeed starts from the current time.
  - The date format should be `YYYY-MM-DD HH:MM:SS`.
  - The cursor can also start from the last garbage collection (GC) timestamp defined in the cluster (default: 4 hours).

- **`initial_scan`** (Optional): A `yes/no` value that determines whether to perform an initial snapshot of the table data. The default value is `no`.
  - **`yes`** — Runs an initial snapshot of the table data.
  - **`no`** — Does not run an initial snapshot of the table data.
## Import Example

`postgresql_crdb_changefeed` supports importing resources.  Supposing the following
Terraform:

```hcl
provider "postgresql" {
  alias = "mycrdb"
}

resource "postgresql_crdb_changefeed" "import_tst" {
  provider = postgresql.mycrdb
  table_list=["yy,dd,bb,cc"]
  avro_schema_prefix="my_avro_prefix"
  kafka_connection_name="my_kafka_connection"
  registry_connection_name="my_registry_connection"
  initial_scan="no"
}
```
After creating the proper changefeed at the cluster, for example with the following command:
```sql
CREATE CHANGEFEED FOR TABLE yy, TABLE dd, TABLE bb, TABLE cc INTO 'external://my_kafka_connection' WITH OPTIONS (avro_schema_prefix = 'my_avro_prefix_', confluent_schema_registry = 'external://my_registry_connection', cursor = '2025-03-19 11:00:00', diff, format = 'avro', initial_scan = 'no', on_error = 'pause', updated)
```
and getting the job_id from the cluster, you can import the changefeed with the following command:

```
$ terraform import "postgresql_crdb_changefeed.import_tst" {job_id}
```
