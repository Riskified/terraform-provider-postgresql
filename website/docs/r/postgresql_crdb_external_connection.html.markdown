---
layout: "postgresql"
page_title: "PostgreSQL: postgresql_crdb_external_connection"
sidebar_current: "docs-postgresql-resource-postgresql_crdb_external_connection"
description: |-
  Creates and manages an external connection.
---

# postgresql_crdb_external_connection

The ``postgresql_crdb_external_connection`` resource creates and manages an external connection.


## Usage

```hcl
resource "postgresql_crdb_external_connection" "confluence_yoav" {
  connection_name = "my_external_conn_name"
  connection_url  = "https://registry_url:9091"
}
```

## Argument Reference

* `connection_name` - (Required) The name of the external connection. Must be unique on the CockroachDB server instance where it is configured.

* `connection_url` - (Required)  
    * The URL of the external connection.  
    * The URL must include proper sink_uri's. for example `https://` or `kafka://` with the port.
for more details https://www.cockroachlabs.com/docs/v25.1/create-external-connection.html#supported-external-storage-and-sinks
    * for example `https://registry_url:9091` or `kafka://kafka:9092`
  
