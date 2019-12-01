package fileexec

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/tail"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/internal/globpath"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/parsers"
	"github.com/influxdata/telegraf/plugins/parsers/csv"
	"github.com/influxdata/telegraf/plugins/parsers/nagios"
	"github.com/kballard/go-shellquote"
)

var (
	offsetsMutex = new(sync.Mutex)
	modTimes     = make(map[string]time.Time)
)

const MaxStderrBytes = 512

type FileExec struct {
	Files []string

	Commands []string
	Command  string
	Timeout  internal.Duration

	parser parsers.Parser

	runner Runner

	Log telegraf.Logger

	modTimes map[string]time.Time

	acc telegraf.Accumulator

	sync.Mutex
}

type Runner interface {
	Run(string, time.Duration) ([]byte, []byte, error)
}

type CommandRunner struct{}

func (c CommandRunner) Run(
	command string,
	timeout time.Duration,
) ([]byte, []byte, error) {
	split_cmd, err := shellquote.Split(command)
	if err != nil || len(split_cmd) == 0 {
		return nil, nil, fmt.Errorf("exec: unable to parse command, %s", err)
	}

	cmd := exec.Command(split_cmd[0], split_cmd[1:]...)

	var (
		out    bytes.Buffer
		stderr bytes.Buffer
	)
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	runErr := internal.RunTimeout(cmd, timeout)

	out = removeCarriageReturns(out)
	if stderr.Len() > 0 {
		stderr = removeCarriageReturns(stderr)
		stderr = truncate(stderr)
	}

	return out.Bytes(), stderr.Bytes(), runErr
}

func truncate(buf bytes.Buffer) bytes.Buffer {
	// Limit the number of bytes.
	didTruncate := false
	if buf.Len() > MaxStderrBytes {
		buf.Truncate(MaxStderrBytes)
		didTruncate = true
	}
	if i := bytes.IndexByte(buf.Bytes(), '\n'); i > 0 {
		// Only show truncation if the newline wasn't the last character.
		if i < buf.Len()-1 {
			didTruncate = true
		}
		buf.Truncate(i)
	}
	if didTruncate {
		buf.WriteString("...")
	}
	return buf
}

// removeCarriageReturns removes all carriage returns from the input if the
// OS is Windows. It does not return any errors.
func removeCarriageReturns(b bytes.Buffer) bytes.Buffer {
	if runtime.GOOS == "windows" {
		var buf bytes.Buffer
		for {
			byt, er := b.ReadBytes(0x0D)
			end := len(byt)
			if nil == er {
				end -= 1
			}
			if nil != byt {
				buf.Write(byt[:end])
			} else {
				break
			}
			if nil != er {
				break
			}
		}
		b = buf
	}
	return b

}

func (e *FileExec) ProcessCommand(command string, acc telegraf.Accumulator, wg *sync.WaitGroup) {
	e.Log.Debug("ProcessCommand() begin")
	e.Log.Infof("ProcessCommand() [%s]", command)

	defer wg.Done()
	_, isNagios := e.parser.(*nagios.NagiosParser)

	out, errbuf, runErr := e.runner.Run(command, e.Timeout.Duration)
	if !isNagios && runErr != nil {
		err := fmt.Errorf("exec: %s for command '%s': %s", runErr, command, string(errbuf))
		acc.AddError(err)
		return
	}

	metrics, err := e.parser.Parse(out)
	if err != nil {
		acc.AddError(err)
		return
	}

	if isNagios {
		metrics, err = nagios.TryAddState(runErr, metrics)
		if err != nil {
			e.Log.Errorf("Failed to add nagios state: %s", err)
		}
	}

	for _, m := range metrics {
		acc.AddMetric(m)
	}

	e.Log.Debug("ProcessCommand() finish")
}

func NewTail() *FileExec {
	offsetsMutex.Lock()
	modTimesCopy := make(map[string]time.Time, len(modTimes))
	for k, v := range modTimes {
		modTimesCopy[k] = v
	}
	offsetsMutex.Unlock()

	return &FileExec{
		//		FromBeginning: false,
		modTimes: modTimesCopy,
		runner:   CommandRunner{},
		Timeout:  internal.Duration{Duration: time.Second * 5},
	}
}

const sampleConfig = `
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
  # updated file path set to {filepath}
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
`

func (t *FileExec) SampleConfig() string {
	return sampleConfig
}

func (t *FileExec) Description() string {
	return "Stream a log file, like the tail -f command"
}

func (e *FileExec) SetParser(parser parsers.Parser) {
	e.parser = parser
}

func (t *FileExec) Gather(acc telegraf.Accumulator) error {
	t.Log.Debug("Gather()")

	t.Lock()
	defer t.Unlock()

	return t.tailNewFiles(true)
}

func (t *FileExec) Start(acc telegraf.Accumulator) error {
	t.Lock()
	defer t.Unlock()

	t.acc = acc

	// initialize
	t.modTimes = make(map[string]time.Time)
	// assumption that once Start is called, all parallel plugins have already been initialized
	offsetsMutex.Lock()
	modTimes = make(map[string]time.Time)
	offsetsMutex.Unlock()

	//	err := t.tailNewFiles(t.FromBeginning)
	err := t.tailNewFiles(false)

	return err
}

func (t *FileExec) tailNewFiles(fromBeginning bool) error {
	t.Log.Debug("tailNewFiles() begin")

	// Create a "tailer" for each file
	for _, filepath := range t.Files {
		g, err := globpath.Compile(filepath)
		if err != nil {
			t.Log.Errorf("Glob %q failed to compile: %s", filepath, err.Error())
		}
		for _, file := range g.Match() {
			fileInfo, _ := os.Stat(file)
			if fileInfo != nil {
				t.Log.Debugf("receiver file:%v", fileInfo.ModTime())
			} else {
				t.Log.Warn("receiver() finish")
			}

			modTime, ok := t.modTimes[file]
			if ok {
				t.Log.Debugf("prev %v", modTime)
			} else {
				t.Log.Infof("new file [%s]", file)

				// 起動時に見つけたファイルにはとりあえず処理しない
				if !fromBeginning {
					offsetsMutex.Lock()
					t.modTimes[file] = fileInfo.ModTime()
					offsetsMutex.Unlock()
					continue
				}
			}

			if fileInfo.ModTime().After(modTime) {
				t.Log.Infof("changed file:[%s]", file)
				t.Log.Infof("receiver file:%v", fileInfo.ModTime())

				// do something
				e := t.NotifyFile(file)
				if e != nil {
					t.acc.AddError(err)
					continue
				}

				t.modTimes[file] = fileInfo.ModTime()
			}
		}
	}

	t.Log.Debug("tailNewFiles() finish")
	return nil
}

func (e *FileExec) NotifyFile(file string) error {
	e.Log.Debug("NotifyFile() begin")

	var wg sync.WaitGroup
	// Legacy single command support
	if e.Command != "" {
		e.Commands = append(e.Commands, e.Command)
		e.Command = ""
	}

	commands := make([]string, 0, len(e.Commands))
	for _, pattern := range e.Commands {
		cmdAndArgs := strings.SplitN(pattern, " ", 2)
		if len(cmdAndArgs) == 0 {
			continue
		}

		matches, err := filepath.Glob(cmdAndArgs[0])
		if err != nil {
			e.acc.AddError(err)
			continue
		}

		if len(matches) == 0 {
			// There were no matches with the glob pattern, so let's assume
			// that the command is in PATH and just run it as it is
			commands = append(commands, pattern)
		} else {
			// There were matches, so we'll append each match together with
			// the arguments to the commands slice
			for _, match := range matches {
				if len(cmdAndArgs) == 1 {
					commands = append(commands, match)
				} else {
					commands = append(commands,
						strings.Join([]string{match, cmdAndArgs[1]}, " "))
				}
			}
		}
	}

	wg.Add(len(commands))
	for _, command := range commands {
		cmd := strings.Replace(command, "{filepath}", file, -1)
		go e.ProcessCommand(cmd, e.acc, &wg)
	}
	wg.Wait()

	e.Log.Debug("NotifyFile() finish")
	return nil
}

// ParseLine parses a line of text.
func parseLine(parser parsers.Parser, line string, firstLine bool) ([]telegraf.Metric, error) {
	switch parser.(type) {
	case *csv.Parser:
		// The csv parser parses headers in Parse and skips them in ParseLine.
		// As a temporary solution call Parse only when getting the first
		// line from the file.
		if firstLine {
			return parser.Parse([]byte(line))
		} else {
			m, err := parser.ParseLine(line)
			if err != nil {
				return nil, err
			}

			if m != nil {
				return []telegraf.Metric{m}, nil
			}
			return []telegraf.Metric{}, nil
		}
	default:
		return parser.Parse([]byte(line))
	}
}

// Receiver is launched as a goroutine to continuously watch a tailed logfile
// for changes, parse any incoming msgs, and add to the accumulator.
func (t *FileExec) receiver(parser parsers.Parser, tailer *tail.Tail) {
	t.Log.Debug("receiver() begin")

	t.Log.Infof("receiver file:[%s]", tailer.Filename)

	fileInfo, _ := os.Stat(tailer.Filename)
	if fileInfo != nil {
		t.Log.Infof("receiver file:%v", fileInfo.ModTime())
	} else {
		t.Log.Warn("receiver() finish")
		return
	}

	t.Log.Debugf("Tail removed for %q", tailer.Filename)

	if err := tailer.Err(); err != nil {
		t.Log.Errorf("Tailing %q: %s", tailer.Filename, err.Error())
	}

	//
	//
	modTime, ok := t.modTimes[tailer.Filename]
	if ok {
		t.Log.Infof("prev %v", modTime)
	} else {
		t.Log.Infof("new file [%s]", tailer.Filename)
		t.modTimes[tailer.Filename] = fileInfo.ModTime()

		t.Log.Info("receiver() finish")
		return
	}

	if fileInfo.ModTime().After(modTime) {
		t.Log.Infof("changed file:[%s]", tailer.Filename)
		t.Log.Infof("receiver file:%v", fileInfo.ModTime())
		t.modTimes[tailer.Filename] = fileInfo.ModTime()
	}

	t.Log.Debug("receiver() finish")
}

func (t *FileExec) Stop() {
	//t.Lock()
	//defer t.Unlock()
}

func init() {
	inputs.Add("fileexec", func() telegraf.Input {
		return NewTail()
	})
}
