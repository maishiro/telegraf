# SQLite3 Output Plugin for Telegraf

This plugin writes to [SQLite3](https://www.sqlite.org/index.html).

## Table Schema

The plugin requires a table with the following schema.


```sql
CREATE TABLE my_metrics (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "timestamp" TEXT,
  "name" TEXT,
  "tags" json,
  "fields" json
);
```

The plugin can create this table for you automatically via the `table_create`
config option, see below.

## Configuration

```toml
# Configuration for CrateDB to send metrics to.
[[outputs.cratedb]]
  # DB file
  file = "./test.db"
  # Timeout for all CrateDB queries.
  timeout = "5s"
  # Name of the table to store metrics in.
  table = "metrics"
  # If true, and the metrics table does not exist, create it automatically.
  table_create = true
```
