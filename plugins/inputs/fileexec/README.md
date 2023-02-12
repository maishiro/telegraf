# FileExec Input Plugin

インターバルごとにファイルの追加、変更を検出。検出したファイルパスをexecの第１引数に渡して実行する。実行した応答文字列をexecのようにparseする。

```
 (exec PATH) (changed file path)
```

telegraf 起動時に既に存在したファイルは更新のみを検出。  
起動後に追加されたファイルは初期の追加も検出。

The plugin expects messages in one of the
[Telegraf Input Data Formats](https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md).

### Configuration:

```toml
# Stream a log file, like the tail -f command
[[inputs.fileexec]]
  ## files to tail.
  ## These accept standard unix glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   "/var/log/**.log"  -> recursively find all .log files in /var/log
  ##   "/var/log/*/*.log" -> find all .log files with a parent dir in /var/log
  ##   "/var/log/apache.log" -> just tail the apache log file
  ##
  ## See https://github.com/gobwas/glob for more examples
  ##
  files = ["/var/mymetrics.out"]

  ## Commands array
  ##   updated file path set to {filepath}
  commands = [
    "/tmp/test.sh {filepath}",
    "/usr/bin/mycollector --foo={filepath}",
    "/tmp/collect_*.sh {filepath}"
  ]

  ## Timeout for each command to complete.
  timeout = "5s"

  ## Data format to consume.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
```

### Metrics:

Metrics are produced according to the `data_format` option.  Additionally a
tag labeled `path` is added to the metric containing the filename being tailed.
