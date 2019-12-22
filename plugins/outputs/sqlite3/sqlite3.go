package sqlite3

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/outputs"

	_ "github.com/mattn/go-sqlite3"
)

const MaxInt64 = int64(^uint64(0) >> 1)

type SQLite3 struct {
	File        string
	Timeout     internal.Duration
	Table       string
	TableCreate bool `toml:"table_create"`
	DB          *sql.DB
}

var sampleConfig = `
  # DB file 
  file = "./test.db"

  # Timeout for all SQLite3 queries.
  timeout = "5s"
  # Name of the table to store metrics in.
  table = "metrics"
  # If true, and the metrics table does not exist, create it automatically.
  table_create = true
`

func (c *SQLite3) Connect() error {
	var dbfile string
	dbfile = c.File
	db, err := sql.Open("sqlite3", dbfile)
	if err != nil {
		return err
	} else if c.TableCreate {
		sql := `
CREATE TABLE IF NOT EXISTS ` + c.Table + ` (
	"id" INTEGER PRIMARY KEY AUTOINCREMENT,
	"timestamp" TEXT,
	"name" TEXT,
	"tags" json,
	"fields" json
);
`
		ctx, cancel := context.WithTimeout(context.Background(), c.Timeout.Duration)
		defer cancel()
		if _, err := db.ExecContext(ctx, sql); err != nil {
			return err
		}
	}
	c.DB = db
	return nil
}

func (c *SQLite3) Write(metrics []telegraf.Metric) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout.Duration)
	defer cancel()
	if sql, err := insertSQL(c.Table, metrics); err != nil {
		return err
	} else if _, err := c.DB.ExecContext(ctx, sql); err != nil {
		return err
	}
	return nil
}

func insertSQL(table string, metrics []telegraf.Metric) (string, error) {
	rows := make([]string, len(metrics))
	for i, m := range metrics {

		cols := []interface{}{
			m.Time().UTC(),
			m.Name(),
			m.Tags(),
			m.Fields(),
		}

		escapedCols := make([]string, len(cols))
		for i, col := range cols {
			escaped, err := escapeValue(col)
			if err != nil {
				return "", err
			}
			escapedCols[i] = escaped
		}
		rows[i] = `(` + strings.Join(escapedCols, ", ") + `)`
	}
	sql := `INSERT INTO ` + table + ` ("timestamp", "name", "tags", "fields")
VALUES
` + strings.Join(rows, " ,\n") + `;`
	fmt.Printf("insertSQL() [%s]", sql)
	return sql, nil
}

// escapeValue returns a string version of val that is suitable for being used
// inside of a VALUES expression or similar. Unsupported types return an error.
//
// Warning: This is not ideal from a security perspective, but unfortunately
// SQLite3 does not support enough of the PostgreSQL wire protocol to allow
// using pgx with $1, $2 placeholders [1]. Security conscious users of this
// plugin should probably refrain from using it in combination with untrusted
// inputs.
//
// [1] https://github.com/influxdata/telegraf/pull/3210#issuecomment-339273371
func escapeValue(val interface{}) (string, error) {
	switch t := val.(type) {
	case string:
		return escapeString(t, `"`), nil
	case int64, float64:
		return fmt.Sprint(t), nil
	case uint64:
		// The long type is the largest integer type in SQLite3 and is the
		// size of a signed int64.  If our value is too large send the largest
		// possible value.
		if t <= uint64(MaxInt64) {
			return strconv.FormatInt(int64(t), 10), nil
		} else {
			return strconv.FormatInt(MaxInt64, 10), nil
		}
	case bool:
		return strconv.FormatBool(t), nil
	case time.Time:
		// see https://crate.io/docs/crate/reference/sql/data_types.html#timestamp
		return escapeValue(t.Format("2006-01-02T15:04:05.999-0700"))
	case map[string]string:
		return escapeObject(convertMap(t))
	case map[string]interface{}:
		return escapeObject(t)
	default:
		// This might be panic worthy under normal circumstances, but it's probably
		// better to not shut down the entire telegraf process because of one
		// misbehaving plugin.
		return "", fmt.Errorf("unexpected type: %T: %#v", t, t)
	}
}

func escapeValue2(val interface{}) (string, error) {
	switch t := val.(type) {
	case string:
		return escapeString(t, `'`), nil
	case int64, float64:
		return fmt.Sprint(t), nil
	case uint64:
		// The long type is the largest integer type in SQLite3 and is the
		// size of a signed int64.  If our value is too large send the largest
		// possible value.
		if t <= uint64(MaxInt64) {
			return strconv.FormatInt(int64(t), 10), nil
		} else {
			return strconv.FormatInt(MaxInt64, 10), nil
		}
	case bool:
		return strconv.FormatBool(t), nil
	case time.Time:
		// see https://crate.io/docs/crate/reference/sql/data_types.html#timestamp
		return escapeValue(t.Format("2006-01-02T15:04:05.999-0700"))
	case map[string]string:
		return escapeObject(convertMap(t))
	case map[string]interface{}:
		return escapeObject2(t)
	default:
		// This might be panic worthy under normal circumstances, but it's probably
		// better to not shut down the entire telegraf process because of one
		// misbehaving plugin.
		return "", fmt.Errorf("unexpected type: %T: %#v", t, t)
	}
}

// convertMap converts m from map[string]string to map[string]interface{} by
// copying it. Generics, oh generics where art thou?
func convertMap(m map[string]string) map[string]interface{} {
	c := make(map[string]interface{}, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

func escapeObject(m map[string]interface{}) (string, error) {
	// There is a decent chance that the implementation below doesn't catch all
	// edge cases, but it's hard to tell since the format seems to be a bit
	// underspecified.
	// See https://crate.io/docs/crate/reference/sql/data_types.html#object

	// We find all keys and sort them first because iterating a map in go is
	// randomized and we need consistent output for our unit tests.
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Now we build our key = val pairs
	pairs := make([]string, 0, len(m))
	for _, k := range keys {
		// escape the value of our key k (potentially recursive)
		//val, err := escapeValue(m[k])
		val, err := escapeValue2(m[k])
		if err != nil {
			return "", err
		}
		pairs = append(pairs, escapeString(k, "'")+":"+val)
	}
	return `"{` + strings.Join(pairs, ", ") + `}"`, nil
}

func escapeObject2(m map[string]interface{}) (string, error) {
	// There is a decent chance that the implementation below doesn't catch all
	// edge cases, but it's hard to tell since the format seems to be a bit
	// underspecified.
	// See https://crate.io/docs/crate/reference/sql/data_types.html#object

	// We find all keys and sort them first because iterating a map in go is
	// randomized and we need consistent output for our unit tests.
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Now we build our key = val pairs
	pairs := make([]string, 0, len(m))
	for _, k := range keys {
		// escape the value of our key k (potentially recursive)
		//val, err := escapeValue(m[k])
		val, err := escapeValue2(m[k])
		if err != nil {
			return "", err
		}
		pairs = append(pairs, escapeString(k, "'")+":"+val)
	}
	return `{` + strings.Join(pairs, ", ") + `}`, nil
}

// escapeString wraps s in the given quote string and replaces all occurrences
// of it inside of s with a double quote.
func escapeString(s string, quote string) string {
	return quote + strings.Replace(s, quote, quote+quote, -1) + quote
}

func (c *SQLite3) SampleConfig() string {
	return sampleConfig
}

func (c *SQLite3) Description() string {
	return "Configuration for SQLite3 to send metrics to."
}

func (c *SQLite3) Close() error {
	return c.DB.Close()
}

func init() {
	outputs.Add("sqlite3", func() telegraf.Output {
		return &SQLite3{
			Timeout: internal.Duration{Duration: time.Second * 5},
		}
	})
}
