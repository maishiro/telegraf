package sqlite3

import (
	"database/sql"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/require"
)

func TestConnectAndWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if os.Getenv("CIRCLE_PROJECT_REPONAME") != "" {
		t.Skip("Skipping test on CircleCI due to docker failures")
	}

	file := testFILE()
	table := "test"

	// dropSQL drops our table before each test. This simplifies changing the
	// schema during development :).
	dropSQL := "DROP TABLE IF EXISTS " + escapeString(table, `"`)
	db, err := sql.Open("sqlite3", file)
	require.NoError(t, err)
	_, err = db.Exec(dropSQL)
	require.NoError(t, err)
	defer db.Close()

	c := &SQLite3{
		File:        file,
		Table:       table,
		Timeout:     internal.Duration{Duration: time.Second * 5},
		TableCreate: true,
	}

	metrics := testutil.MockMetrics()
	require.NoError(t, c.Connect())
	require.NoError(t, c.Write(metrics))

	// The code below verifies that the metrics were written. We have to select
	// the rows using their primary keys in order to take advantage of
	// read-after-write consistency in CrateDB.
	for _, m := range metrics {
		timestamp, err := escapeValue(m.Time())
		require.NoError(t, err)

		//var id int64
		var name string
		row := db.QueryRow(
			"SELECT name FROM " + escapeString(table, `"`) + " " +
				"WHERE timestamp = " + timestamp,
		)
		require.NoError(t, row.Scan(&name))
		// We could check the whole row, but this is meant to be more of a smoke
		// test, so just checking the HashID seems fine.
		require.Equal(t, name, m.Name())
	}

	require.NoError(t, c.Close())
}

func Test_insertSQL(t *testing.T) {
	tests := []struct {
		Metrics []telegraf.Metric
		Want    string
	}{
		{
			Metrics: testutil.MockMetrics(),
			Want: strings.TrimSpace(`
INSERT INTO my_table ('timestamp', 'name', 'tags', 'fields')
VALUES
('2009-11-10T23:00:00+0000', 'test1', '{"tag1":"value1"}', '{"value":1}');
`),
		},
	}

	for _, test := range tests {
		if got, err := insertSQL("my_table", test.Metrics); err != nil {
			t.Error(err)
		} else if got != test.Want {
			t.Errorf("got:\n%s\n\nwant:\n%s", got, test.Want)
		}
	}
}

func Test_escapeValue(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if os.Getenv("CIRCLE_PROJECT_REPONAME") != "" {
		t.Skip("Skipping test on CircleCI due to docker failures")
	}

	tests := []struct {
		Val  interface{}
		Want string
	}{
		// string
		{`foo`, `'foo'`},
		{`foo'bar 'yeah`, `'foo''bar ''yeah'`},
		// int types
		{int64(123), `123`},
		{uint64(123), `123`},
		{uint64(MaxInt64) + 1, `9223372036854775807`},
		{true, `true`},
		{false, `false`},
		// float types
		{float64(123.456), `123.456`},
		// time.Time
		{time.Date(2017, 8, 7, 16, 44, 52, 123*1000*1000, time.FixedZone("Dreamland", 5400)), `'2017-08-07T16:44:52.123+0130'`},
		// map[string]string
		{map[string]string{}, `'{}'`},
		{map[string]string(nil), `'{}'`},
		{map[string]string{"foo": "bar"}, `'{"foo":"bar"}'`},
		{map[string]string{"foo": "bar", "one": "more"}, `'{"foo":"bar", "one":"more"}'`},
		// map[string]interface{}
		{map[string]interface{}{}, `'{}'`},
		{map[string]interface{}(nil), `'{}'`},
		{map[string]interface{}{"foo": "bar"}, `'{"foo":"bar"}'`},
		{map[string]interface{}{"foo": "bar", "one": "more"}, `'{"foo":"bar", "one":"more"}'`},
		{map[string]interface{}{"foo": map[string]interface{}{"one": "more"}}, `'{"foo":{"one":"more"}}'`},
		//{map[string]interface{}{`fo"o`: `b'ar`, `ab'c`: `xy"z`, `on"""e`: `mo'''re`}, `'{"ab'c":"xy""z", "fo""o":"b'ar", "on""""""e":"mo'''re"}'`},
	}

	file := testFILE()
	db, err := sql.Open("sqlite3", file)
	require.NoError(t, err)
	defer db.Close()

	for _, test := range tests {
		got, err := escapeValue(test.Val)
		if err != nil {
			t.Errorf("val: %#v: %s", test.Val, err)
		} else if got != test.Want {
			t.Errorf("got:\n%s\n\nwant:\n%s", got, test.Want)
		}

		// This is a smoke test that will blow up if our escaping causing a SQL
		// syntax error, which may allow for an attack.
		var reply interface{}
		row := db.QueryRow("SELECT " + got)
		require.NoError(t, row.Scan(&reply))
	}
}

func testURL() string {
	url := os.Getenv("CRATE_URL")
	if url == "" {
		return "postgres://" + testutil.GetLocalHost() + ":6543/test?sslmode=disable"
	}
	return url
}

func testFILE() string {
	file := os.Getenv("SQLITE3_FILE")
	if file == "" {
		return "./test.db"
	}
	return file
}

func TestGetTagKeys(t *testing.T) {
	e := &SQLite3{
		DefaultTagValue: "none",
	}

	var tests = []struct {
		File              string
		ExpectedIndexName string
		ExpectedTagKeys   []string
	}{
		{
			"indexname",
			"indexname",
			[]string{},
		}, {
			"indexname-%Y",
			"indexname-%Y",
			[]string{},
		}, {
			"indexname-%Y-%m",
			"indexname-%Y-%m",
			[]string{},
		}, {
			"indexname-%Y-%m-%d",
			"indexname-%Y-%m-%d",
			[]string{},
		}, {
			"indexname-%Y-%m-%d-%H",
			"indexname-%Y-%m-%d-%H",
			[]string{},
		}, {
			"indexname-%y-%m",
			"indexname-%y-%m",
			[]string{},
		}, {
			"indexname-{{tag1}}-%y-%m",
			"indexname-%s-%y-%m",
			[]string{"tag1"},
		}, {
			"indexname-{{tag1}}-{{tag2}}-%y-%m",
			"indexname-%s-%s-%y-%m",
			[]string{"tag1", "tag2"},
		}, {
			"indexname-{{tag1}}-{{tag2}}-{{tag3}}-%y-%m",
			"indexname-%s-%s-%s-%y-%m",
			[]string{"tag1", "tag2", "tag3"},
		},
	}
	for _, test := range tests {
		indexName, tagKeys := e.GetTagKeys(test.File)
		if indexName != test.ExpectedIndexName {
			t.Errorf("Expected indexname %s, got %s\n", test.ExpectedIndexName, indexName)
		}
		if !reflect.DeepEqual(tagKeys, test.ExpectedTagKeys) {
			t.Errorf("Expected tagKeys %s, got %s\n", test.ExpectedTagKeys, tagKeys)
		}
	}

}

func TestGetFileName(t *testing.T) {
	e := &SQLite3{
		DefaultTagValue: "none",
	}

	var tests = []struct {
		EventTime time.Time
		Tags      map[string]string
		TagKeys   []string
		File      string
		Expected  string
	}{
		{
			time.Date(2014, 12, 01, 23, 30, 00, 00, time.UTC),
			map[string]string{"tag1": "value1", "tag2": "value2"},
			[]string{},
			"indexname",
			"indexname",
		},
		{
			time.Date(2014, 12, 01, 23, 30, 00, 00, time.UTC),
			map[string]string{"tag1": "value1", "tag2": "value2"},
			[]string{},
			"indexname-%Y",
			"indexname-2014",
		},
		{
			time.Date(2014, 12, 01, 23, 30, 00, 00, time.UTC),
			map[string]string{"tag1": "value1", "tag2": "value2"},
			[]string{},
			"indexname-%Y-%m",
			"indexname-2014-12",
		},
		{
			time.Date(2014, 12, 01, 23, 30, 00, 00, time.UTC),
			map[string]string{"tag1": "value1", "tag2": "value2"},
			[]string{},
			"indexname-%Y-%m-%d",
			"indexname-2014-12-01",
		},
		{
			time.Date(2014, 12, 01, 23, 30, 00, 00, time.UTC),
			map[string]string{"tag1": "value1", "tag2": "value2"},
			[]string{},
			"indexname-%Y-%m-%d-%H",
			"indexname-2014-12-01-23",
		},
		{
			time.Date(2014, 12, 01, 23, 30, 00, 00, time.UTC),
			map[string]string{"tag1": "value1", "tag2": "value2"},
			[]string{},
			"indexname-%y-%m",
			"indexname-14-12",
		},
		{
			time.Date(2014, 12, 01, 23, 30, 00, 00, time.UTC),
			map[string]string{"tag1": "value1", "tag2": "value2"},
			[]string{},
			"indexname-%Y-%V",
			"indexname-2014-49",
		},
		{
			time.Date(2014, 12, 01, 23, 30, 00, 00, time.UTC),
			map[string]string{"tag1": "value1", "tag2": "value2"},
			[]string{"tag1"},
			"indexname-%s-%y-%m",
			"indexname-value1-14-12",
		},
		{
			time.Date(2014, 12, 01, 23, 30, 00, 00, time.UTC),
			map[string]string{"tag1": "value1", "tag2": "value2"},
			[]string{"tag1", "tag2"},
			"indexname-%s-%s-%y-%m",
			"indexname-value1-value2-14-12",
		},
		{
			time.Date(2014, 12, 01, 23, 30, 00, 00, time.UTC),
			map[string]string{"tag1": "value1", "tag2": "value2"},
			[]string{"tag1", "tag2", "tag3"},
			"indexname-%s-%s-%s-%y-%m",
			"indexname-value1-value2-none-14-12",
		},
	}
	for _, test := range tests {
		indexName := e.GetFileName(test.File, test.EventTime, test.TagKeys, test.Tags)
		if indexName != test.Expected {
			t.Errorf("Expected indexname %s, got %s\n", test.Expected, indexName)
		}
	}
}
