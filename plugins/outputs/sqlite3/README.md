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
  ## The metric timestamp will be used to decide the destination file name
  # %Y - year (2016)
  # %y - last two digits of year (00..99)
  # %m - month (01..12)
  # %d - day of month (e.g., 01)
  # %H - hour (00..23)
  # %V - week of the year (ISO week) (01..53)
  ## Additionally, you can specify a tag name using the notation {{tag_name}}
  ## which will be used as part of the index name. If the tag does not exist,
  ## the default tag value will be used.
  # default_tag_value = "none"
  #file = "./test_%Y%m%d_%H00.db"
  file = "./test_%Y%m%d.db" # required.
  # Timeout for all CrateDB queries.
  timeout = "5s"
  # Name of the table to store metrics in.
  table = "metrics"
  # If true, and the metrics table does not exist, create it automatically.
  table_create = true
```
