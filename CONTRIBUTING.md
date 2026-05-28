# Contributing

## Build

```bash
mvn clean verify             # build + run unit tests
mvn verify -DskipITs=false   # also run integration tests (requires .env)
```

## Integration tests

`*IT.java` integration tests hit a real Azure Databricks workspace. Place a `.env` at the repo root:

```
DATABRICKS_HOST=adb-XXXXXXXXXXXXXXXX.X.azuredatabricks.net
DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/XXXXXXXXXXXX
DATABRICKS_CATALOG=main
DATABRICKS_SCHEMA=dbz_it
```

The PAT identity needs `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `SELECT`, and `MODIFY` on the schema, plus `CAN USE` on the warehouse. ITs create and drop their own tables.

`.env` is gitignored. The same five values map onto GitHub Actions repository secrets for nightly CI.

## Layout

```
src/main/java/io/debezium/connector/databricks/
├── DatabricksConnector             — Kafka Connect SourceConnector entrypoint
├── DatabricksConnectorTask         — SourceTask: lifecycle + poll() loop
├── DatabricksConnectorConfig       — Field declarations + typed accessors
├── DatabricksPartition / OffsetContext / SourceInfo
├── DatabricksSchema                — per-table Connect schema cache + Envelope
├── DatabricksRecordBuilder         — Builds Debezium-envelope SourceRecords
├── cdf/                            — CDF reader (table_changes), schema inspection, history scanner
├── connection/                     — Databricks JDBC wrapper
│   └── auth/                       — PAT, Entra SP, Azure MI auth providers
├── metrics/                        — JMX MBean
└── source/DatabricksPoller         — Drives snapshot + streaming for all tables
```

## Conventions

- Apache 2.0 header in every `.java` file (see existing files for the template).
- `*Test.java` = unit, runs under Surefire on every build. `*IT.java` = integration, runs under Failsafe.
- The connector targets the **Debezium Embedded Engine** as the primary runtime — never depend on Kafka Connect runtime classes; never assume Kafka is present at runtime.

## Release

Releases are tag-driven. Pushing a tag `vX.Y.Z` runs `.github/workflows/release.yml` which:

1. Sets the Maven version to `X.Y.Z`
2. Builds + tests + signs (if release profile active)
3. Deploys jar (+ sources + javadoc) to GitHub Packages
4. Creates a GitHub Release with the jars attached
