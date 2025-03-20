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
resource "postgresql_crdb_external_connection" "my_confluence" {
  connection_name = "my_external_conn_name"
  connection_url  = "https://registry_url:9091"
}
```

## Argument Reference

- **`connection_name`** (Required): Specifies the name of the external connection. This name must be unique within the CockroachDB server instance where it's set up.

- **`connection_url`** (Required):
  - Defines the URL for the external connection.
  - The URL must include a valid sink URI, such as `https://` or `kafka://`, followed by the appropriate port number.
  - Example URLs: `https://registry_url:9091` or `kafka://kafka:9092`.

For more details, refer to the [CockroachDB documentation](https://www.cockroachlabs.com/docs/v25.1/create-external-connection.html#supported-external-storage-and-sinks).