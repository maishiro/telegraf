package postgres

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

	_ "github.com/lib/pq"
)

const MaxInt64 = int64(^uint64(0) >> 1)

type Postgres struct {
	URL     string
	Timeout internal.Duration
	Table   string

	DB        *sql.DB
	TableName string
	Log       telegraf.Logger
}

var sampleConfig = `
  # A github.com/jackc/pgx connection string.
  # See https://godoc.org/github.com/jackc/pgx#ParseDSN
  url = "postgres://user:password@localhost/schema?sslmode=disable"
  
  # Timeout for all rdms queries.
  timeout = "5s"
  # Name of the table to store metrics in.
  table = "metrics"
`

func (c *Postgres) Connect() error {
	db, err := sql.Open("postgres", c.URL)
	if err != nil {
		return err
	} else {
		sql := `
			CREATE TABLE IF NOT EXISTS ` + c.Table + ` (
				datetime TIMESTAMP WITH TIME ZONE NOT NULL,
				name TEXT NOT NULL,
				tags JSONB NOT NULL,
				fields JSONB NOT NULL
			) PARTITION BY RANGE( datetime );`

		c.Log.Debugf("D! [%s]", sql)
		ctx, cancel := context.WithTimeout(context.Background(), c.Timeout.Duration)
		defer cancel()
		if _, err := db.ExecContext(ctx, sql); err != nil {
			return err
		}
	}
	c.DB = db
	return nil
}

func (c *Postgres) Write(metrics []telegraf.Metric) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout.Duration)
	defer cancel()

	m := make(map[string][]telegraf.Metric)
	for _, metric := range metrics {
		tableName := c.GetTableName(c.Table, metric.Time(), []string{}, metric.Tags())
		fmt.Printf("Write() tableName : [%s]", tableName)
		m[tableName] = append(m[tableName], metric)
	}

	for k, mtrcs := range m {
		tableName := k
		metric := mtrcs[0]

		if sql1, err := insertSQL(c.Table, mtrcs); err != nil {
			return err
			//		} else if _, err := c.DB.ExecContext(ctx, sql); err != nil {
		} else {

			// fileName := c.GetFileName(c.File, metric.Time(), c.TagKeys, metric.Tags())
			// fmt.Printf("Write() fileName : [%s]", fileName)

			if c.TableName != tableName {
				sql := `CREATE TABLE IF NOT EXISTS ` + tableName + ` PARTITION OF ` + c.Table +
					` FOR VALUES FROM ('` + metric.Time().Format("2006-01-02") + `') TO ('` + metric.Time().Add(time.Duration(24)*time.Hour).Format("2006-01-02") + `');`
				fmt.Printf("Write() [%s]", sql)
				c.Log.Debugf("Write() [%s]", sql)

				ctx, cancel := context.WithTimeout(context.Background(), c.Timeout.Duration)
				defer cancel()
				if _, err := c.DB.ExecContext(ctx, sql); err != nil {
					return err
				}

				c.TableName = tableName
			}

			if _, err := c.DB.ExecContext(ctx, sql1); err != nil {
				return err
			}
		}
	}

	return nil
}

func insertSQL(table string, metrics []telegraf.Metric) (string, error) {
	rows := make([]string, len(metrics))
	for i, m := range metrics {

		cols := []interface{}{
			m.Time(),
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
	sql := `INSERT INTO ` + table + ` (datetime, name, tags, fields)
	VALUES
	` + strings.Join(rows, " ,\n") + `;`
	fmt.Printf("insertSQL() [%s]", sql)
	return sql, nil
}

func (a *Postgres) GetTableName(indexName string, eventTime time.Time, tagKeys []string, metricTags map[string]string) string {
	return indexName + "_" + eventTime.Format("20060102")
}

// escapeValue returns a string version of val that is suitable for being used
// inside of a VALUES expression or similar. Unsupported types return an error.
//
// Warning: This is not ideal from a security perspective, but unfortunately
// rdms does not support enough of the PostgreSQL wire protocol to allow
// using pgx with $1, $2 placeholders [1]. Security conscious users of this
// plugin should probably refrain from using it in combination with untrusted
// inputs.
//
// [1] https://github.com/influxdata/telegraf/pull/3210#issuecomment-339273371
func escapeValue(val interface{}) (string, error) {
	switch t := val.(type) {
	case string:
		return escapeString(t, `'`), nil
	case int64, float64:
		return fmt.Sprint(t), nil
	case uint64:
		// The long type is the largest integer type in rdms and is the
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
		return escapeValue(t.Format("2006-01-02 15:04:05.999-0700"))
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
		return escapeString(t, `"`), nil
	case int64, float64:
		return fmt.Sprint(t), nil
	case uint64:
		// The long type is the largest integer type in rdms and is the
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
		return escapeValue(t.Format("2006-01-02 15:04:05.999-0700"))
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
		pairs = append(pairs, escapeString(k, `"`)+":"+val)
	}
	return `'{` + strings.Join(pairs, ", ") + `}'`, nil
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
		pairs = append(pairs, escapeString(k, `"`)+":"+val)
	}
	return `{` + strings.Join(pairs, ", ") + `}`, nil
}

// escapeString wraps s in the given quote string and replaces all occurrences
// of it inside of s with a double quote.
func escapeString(s string, quote string) string {
	return quote + strings.Replace(s, quote, quote+quote, -1) + quote
}

func (c *Postgres) SampleConfig() string {
	return sampleConfig
}

func (c *Postgres) Description() string {
	return "Configuration for rdms to send metrics to."
}

func (c *Postgres) Close() error {
	return c.DB.Close()
}

func init() {
	outputs.Add("postgres", func() telegraf.Output {
		return &Postgres{
			Timeout: internal.Duration{Duration: time.Second * 5},
		}
	})
}
