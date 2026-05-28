# Debezium Connector for Azure Databricks

A [Debezium](https://debezium.io) source connector that captures row-level change events from [Delta Lake](https://delta.io) tables on Azure Databricks via **Change Data Feed (CDF)**.

Designed primarily for the **[Debezium Embedded Engine](https://debezium.io/documentation/reference/stable/development/engine.html)**; also runs in Debezium Server and Kafka Connect.

## Status

`0.1.x` — initial release. Targets **Debezium 3.1.x**, Java 17+.

## Features

- **Delta Lake Change Data Feed** as the source mechanism — captures `insert`, `update_preimage` + `update_postimage`, `delete` from any Delta table with `delta.enableChangeDataFeed = true`.
- **Snapshot modes**: `never`, `initial`, `initial_only` via Delta time travel (`VERSION AS OF`).
- **TRUNCATE / REPLACE / RESTORE detection** via `DESCRIBE HISTORY` scan, emitted as Debezium truncate events.
- **Schema evolution**: additive changes (column add, type widening) adapt transparently; non-additive changes emit a schema-change event and trigger a re-snapshot at the next safe version.
- **Authentication**: Personal Access Token, Azure Entra ID service principal, Azure Managed Identity.
- **Embedded-engine first**: no dependency on Kafka or Kafka Connect runtime at runtime.

## Quickstart — Debezium Embedded Engine

```xml
<dependency>
    <groupId>com.recordpoint.debezium</groupId>
    <artifactId>debezium-connector-databricks</artifactId>
    <version>0.1.0</version>
</dependency>
```

```java
Properties props = new Properties();
props.setProperty("name", "databricks-orders");
props.setProperty("connector.class", "io.debezium.connector.databricks.DatabricksConnector");
props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
props.setProperty("offset.storage.file.filename", "/var/lib/debezium/offsets.dat");
props.setProperty("offset.flush.interval.ms", "10000");

props.setProperty("topic.prefix", "prod-databricks");
props.setProperty("databricks.workspace.host", "adb-xxxxx.x.azuredatabricks.net");
props.setProperty("databricks.warehouse.http.path", "/sql/1.0/warehouses/abc123");
props.setProperty("databricks.auth.type", "pat");
props.setProperty("databricks.token", System.getenv("DATABRICKS_TOKEN"));
props.setProperty("databricks.catalog", "main");
props.setProperty("schema.include.list", "main\\.banking");
props.setProperty("table.include.list", "main\\.banking\\.(customer|account|transaction)");
props.setProperty("snapshot.mode", "initial");

try (DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine
        .create(Json.class)
        .using(props)
        .notifying(record -> System.out.println(record))
        .build()) {
    Executors.newSingleThreadExecutor().execute(engine);
    // ... your application
}
```

## Authentication

| `databricks.auth.type` | Required properties |
|---|---|
| `pat` | `databricks.token` |
| `azure-entra-sp` | `databricks.entra.tenant.id`, `databricks.entra.client.id`, `databricks.entra.client.secret` |
| `azure-mi` | `databricks.azure.workspace.resource.id`; optionally `databricks.mi.client.id` for user-assigned MI |

Token refresh is handled by the underlying Databricks JDBC driver for OAuth/MI modes.

## Configuration

See [`DatabricksConnectorConfig`](src/main/java/io/debezium/connector/databricks/DatabricksConnectorConfig.java) for the full list.

## Required Databricks permissions

The connector identity (PAT owner or service principal) needs:

```sql
GRANT USE CATALOG ON CATALOG  <catalog>           TO `<identity>`;
GRANT USE SCHEMA  ON SCHEMA   <catalog>.<schema>  TO `<identity>`;
GRANT SELECT      ON SCHEMA   <catalog>.<schema>  TO `<identity>`;  -- blanket
-- and on the SQL warehouse:
-- GRANT CAN USE ON WAREHOUSE <warehouse-id> TO `<identity>`;
```

Each source table must have `delta.enableChangeDataFeed = true`:

```sql
ALTER TABLE <catalog>.<schema>.<table>
  SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

## Building from source

```
mvn clean verify             # unit tests
mvn verify -Pit              # + integration tests (requires .env)
```

`.env` must contain `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_CATALOG`, `DATABRICKS_SCHEMA`. See [`.env.example`](.env.example).

## License

Apache License 2.0. See [LICENSE](LICENSE).

## Disclaimer

Not endorsed by Debezium, Apache Kafka, Microsoft, or Databricks.
