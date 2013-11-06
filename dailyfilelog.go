// Copyright (C) 2010, Kyle Lemons <kyle@kylelemons.net>.  All rights reserved.

package log4go

import (
	"fmt"
	"os"
  "os/exec"
  "path/filepath"
	"time"
)

// This log writer sends output to a file
type DailyFileLogWriter struct {
	rec chan *LogRecord
	rot chan bool

	// The opened file
	filename string
	filedate time.Time "UTC timestamp the file was created / rotated at"
	file     *os.File

	// The logging format
	format string

	// File header/trailer
	header, trailer string

	// How many old logfiles to keep
	rotate_limit uint64 "Number of days to keep, 0=all"
}

// This is the DailyFileLogWriter's output method
func (w *DailyFileLogWriter) LogWrite(rec *LogRecord) {
	w.rec <- rec
}

func (w *DailyFileLogWriter) Close() {
	close(w.rec)
	w.file.Sync()
}

// NewDailyFileLogWriter creates a new LogWriter which writes to the given file and
// has rotation enabled if rotate is true.
//
// If rotate is true, any time a new log file is opened, the old one is renamed
// with a .### extension to preserve it.  The various Set* methods can be used
// to configure log rotation based on lines, size, and daily.
//
// The standard log-line format is:
//   [%D %T] [%L] (%S) %M
func NewDailyFileLogWriter(fname string, rotate_limit uint64) *DailyFileLogWriter {
	w := &DailyFileLogWriter{
		rec:          make(chan *LogRecord, LogBufferLength),
		rot:          make(chan bool),
		filename:     fname,
		filedate:     time.Now().UTC(),
		format:       "[%D %T] [%L] (%S) %M",
		rotate_limit: rotate_limit,
	}

	// open the file for the first time
	if err := w.intRotate(); err != nil {
    panic(err)
		fmt.Fprintf(os.Stderr, "DailyFileLogWriter(%q): %s\n", w.filename, err)
		return nil
	}

	go func() {
		defer func() {
			if w.file != nil {
				fmt.Fprint(w.file, FormatLogRecord(w.trailer, &LogRecord{Created: time.Now()}))
				w.file.Close()
			}
		}()

		for {
			select {
			case <-w.rot:
				if err := w.intRotate(); err != nil {
					fmt.Fprintf(os.Stderr, "DailyFileLogWriter(%q): %s\n", w.filename, err)
					return
				}
			case rec, ok := <-w.rec:
				if !ok {
					return
				}
				if time.Now().UTC().Day() != w.filedate.Day() {
					if err := w.intRotate(); err != nil {
						fmt.Fprintf(os.Stderr, "DailyFileLogWriter(%q): %s\n", w.filename, err)
						return
					}
				}

				// Perform the write
				_, err := fmt.Fprint(w.file, FormatLogRecord(w.format, rec))
				if err != nil {
					fmt.Fprintf(os.Stderr, "DailyFileLogWriter(%q): %s\n", w.filename, err)
					return
				}
			}
		}
	}()

	return w
}

// Request that the logs rotate
func (w *DailyFileLogWriter) Rotate() {
	w.rot <- true
}

// If this is called in a threaded context, it MUST be synchronized
func (w *DailyFileLogWriter) intRotate() error {
	// Close any log file that may be open
	if w.file != nil {
		fmt.Fprint(w.file, FormatLogRecord(w.trailer, &LogRecord{Created: time.Now()}))
		w.file.Close()
	}

	// If we are keeping log files, move it to the next available number
	if fi, err := os.Lstat(w.filename); (nil != fi || os.IsExist(err)) && w.rotate_limit > 0  {
		// <filename>.YYYY-MM-DD
		fname := fmt.Sprintf("%s.%04d-%02d-%02d", w.filename, w.filedate.Year(), w.filedate.Month(), w.filedate.Day())

		// Rename the file to its newfound home
		if err := os.Rename(w.filename, fname); err != nil {
			return fmt.Errorf("Rotate: %s\n", err)
		} else {
      // Debugging:
      
      // fmt.Fprintf(os.Stderr, "DailyFileLogWriter[intRotate](%v, %v) -> %v\n", w.filename, w.filedate, fname)
		}
    
    cmd := exec.Command("gzip", "--fast", "--force", fname)
    cmd.Dir = filepath.Dir(fname)
    if err := cmd.Run(); nil != err {
      return err
    }
	}

	// Open the log file
	fd, err := os.OpenFile(w.filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return err
	}
	w.filedate = time.Now().UTC()
	w.file = fd

	fmt.Fprint(w.file, FormatLogRecord(w.header, &LogRecord{Created: w.filedate}))

	return nil
}

// Set the logging format (chainable).  Must be called before the first log
// message is written.
func (w *DailyFileLogWriter) SetFormat(format string) *DailyFileLogWriter {
	w.format = format
	return w
}

// Set the logfile header and footer (chainable).  Must be called before the first log
// message is written.  These are formatted similar to the FormatLogRecord (e.g.
// you can use %D and %T in your header/footer for date and time).
func (w *DailyFileLogWriter) SetHeadFoot(head, foot string) *DailyFileLogWriter {
	w.header, w.trailer = head, foot
	if w.file != nil {
		fmt.Fprint(w.file, FormatLogRecord(w.header, &LogRecord{Created: time.Now()}))
	}
	return w
}

// SetRotateLimit changes whether or not the old logs are kept. (chainable) Must be
// called before the first log message is written.  If rotate is 0, the
// files are overwritten; otherwise, they are rotated to another file before the
// new log is opened.
func (w *DailyFileLogWriter) SetRotateLimit(rotate_limit uint64) *DailyFileLogWriter {
	w.rotate_limit = rotate_limit
	return w
}

// NewXMLLogWriter is a utility method for creating a DailyFileLogWriter set up to
// output XML record log messages instead of line-based ones.
func NewDailyXMLLogWriter(fname string, rotate_limit uint64) *DailyFileLogWriter {
	return NewDailyFileLogWriter(fname, rotate_limit).SetFormat(
		`	<record level="%L">
		<timestamp>%D %T</timestamp>
		<source>%S</source>
		<message>%M</message>
	</record>`).SetHeadFoot("<log created=\"%D %T\">", "</log>")
}
