package readers

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"time"
)

const (
	IBMDockerRegistryTimePattern  = "2006-01-02 15:04:05.000"
	IBMDockerRegistryTimePattern2 = "2006-01-02 15:04:05"
)

type IBMDockerRegistryReader struct {
	backend *csv.Reader
	cursor  int
}

func NewIBMDockerRegistryReader(rd io.Reader) *IBMDockerRegistryReader {
	return &IBMDockerRegistryReader{backend: csv.NewReader(bufio.NewReader(rd))}
}

func (reader *IBMDockerRegistryReader) Read() (*Record, error) {
	if reader.cursor == 0 {
		// Skip first line
		_, err := reader.backend.Read()
		if err != nil {
			return nil, err
		}

		reader.cursor++
	}

	line, err := reader.backend.Read()
	if err != nil {
		return nil, err
	}

	rec := &Record{}
	reader.cursor++

	rec.Key = line[6]
	sz, szErr := strconv.ParseFloat(line[9], 64)
	if szErr == nil {
		rec.Size = uint64(sz)
	}
	ts, tErr := time.Parse(IBMDockerRegistryTimePattern, line[11][:len(IBMDockerRegistryTimePattern)])
	if tErr != nil {
		ts, tErr = time.Parse(IBMDockerRegistryTimePattern2, line[11][:len(IBMDockerRegistryTimePattern2)])
	}
	if tErr == nil {
		rec.Timestamp = ts.UnixNano()
	}

	if szErr != nil || tErr != nil {
		rec.Error = fmt.Errorf("error on parse record, skip line %d: %v(%v, %v)", reader.cursor, line, szErr, tErr)
	}
	return rec, nil
}

func (reader *IBMDockerRegistryReader) Report() []string {
	return nil
}
