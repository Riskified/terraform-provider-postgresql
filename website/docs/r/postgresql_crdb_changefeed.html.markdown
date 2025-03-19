---
layout: "postgresql"
page_title: "PostgreSQL: postgresql_crdb_changefeed"
sidebar_current: "docs-postgresql-resource-postgresql_crdb_changefeed"
description: |-
  Creates and manages a change-feed.
---

# postgresql_crdb_changefeed

The ``postgresql_crdb_changefeed`` resource creates and manages a change-feed.
* Note that you need to use the `SET CLUSTER SETTING kv.rangefeed.enabled = true` command on your cluster.


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

* `table_list` - (Required) a list of tables to include in the changefeed. The tables must exists in the database.

* `avro_schema_prefix` - (Required) The prefix to use for the avro schema. The changefeed will be created with a `_` at the end. 
for example `avro_schema_prefix = 'my_avro_prefix_'` 

* `kafka_connection_name` - (Required) external connection to the kafka cluster. The connection must exists in the database.
This can also be an output from the `postgresql_external_connection` resource.
see https://www.cockroachlabs.com/docs/v25.1/create-external-connection.html for details

* `registry_connection_name` - (Required) external connection to the schema registry. The connection must exists in the database.
This can also be an output from the `postgresql_external_connection` resource.
see https://www.cockroachlabs.com/docs/v25.1/create-external-connection.html for details

* `start_from` - (Optional) Timestamp for `cursor` to start from. If left empty, the changefeed will start from the current time. cursor can start from the last GC defined at the cluster (default 4 hours)

* `initial_scan` - (Required) `yes/no` value  
    * `yes` - will run initial snapshot the data from the tables.
    * `no` - will not run initial snapshot the data from the tables.
## Import Example

`postgresql_crdb_changefeed` supports importing resources.  Supposing the following
Terraform:

```hcl
provider "postgresql" {
  alias = "mycrdb"
}

resource "postgresql_crdb_changefeed" "import_tst" {
  provider = postgresql.mycrdb
  table_list=["yy"]
  avro_schema_prefix="my_avro_prefix"
  kafka_connection_name="my_kafka_connection"
  registry_connection_name="my_registry_connection"
  start_from=""
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
