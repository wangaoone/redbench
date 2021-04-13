package readers

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"sync"
	"time"
)

var (
	IBMObjectStoreMethodPattern                   = regexp.MustCompile(`^REST\.([A-Z]+)\.OBJECT$`)
	ErrUnexpectedIBMObjectStoreMethod             = errors.New("unexpected method")
	ErrUnexpectedIBMObjectStoreFragment           = errors.New("unexpected fragment found for non-GET method")
	ErrIgnoreIBMObjectStoreFragment               = errors.New("ignore fragment trace")
	ErrUnexpectedIBMObjectStoreOverlappedFragment = errors.New("unexpected fragment trace")
)

// type fragmentTracer struct {
// 	Key  string
// 	Size uint64
// 	Seen uint64
// 	Map  []uint64
// }

type IBMObjectStoreReader struct {
	*BaseReader

	backend *csv.Reader
	cursor  int
	// incompleted map[string]*fragmentTracer
	// incompletedSeen int
	pool *sync.Pool
}

func NewIBMObjectStoreReader(rd io.Reader) *IBMObjectStoreReader {
	reader := &IBMObjectStoreReader{
		BaseReader: NewBaseReader(),
		backend:    csv.NewReader(bufio.NewReader(rd)),
		// incompleted: make(map[string]*fragmentTracer, 100),
		pool: &sync.Pool{
			New: func() interface{} {
				return &Record{}
			},
		},
	}
	reader.backend.Comma = ' '          // Space separated.
	reader.backend.FieldsPerRecord = -1 // Variable number of fields.
	return reader
}

func (reader *IBMObjectStoreReader) Read() (*Record, error) {
	line, err := reader.backend.Read()
	if err != nil {
		return nil, err
	}

	rec, _ := reader.BaseReader.Read()
	reader.cursor++

	if len(line) < 3 {
		rec.Error = fmt.Errorf("invalid record, line %d: %v", reader.cursor, line)
		return rec, nil
	}

	err = reader.validate(line, rec)
	if err != nil {
		rec.Error = fmt.Errorf("error on process record, skip line %d: %v(%v)", reader.cursor, line, err)
	}
	return rec, nil
}

func (reader *IBMObjectStoreReader) Report() []string {
	return []string{"Incomplete fragments: disabled"}
	// return []string{fmt.Sprintf("Incomplete fragments: %d", len(reader.incompleted))}
}

func (reader *IBMObjectStoreReader) validate(fields []string, rec *Record) (err error) {
	// Parse timestamp
	rec.Timestamp, err = strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return
	}
	rec.Timestamp = rec.Timestamp * int64(time.Millisecond)

	// Parse method
	matches := IBMObjectStoreMethodPattern.FindStringSubmatch(fields[1])
	if len(matches) == 0 {
		return ErrUnexpectedIBMObjectStoreMethod
	}
	rec.Method = matches[1]

	// Key
	rec.Key = fmt.Sprintf("/ibm/objectstore/%s", fields[2])

	// Parse size
	if len(fields) > 3 {
		rec.Size, err = strconv.ParseUint(fields[3], 10, 64)
		if err != nil {
			return
		}
	}

	// Fragment
	if len(fields) > 4 {
		if rec.Method != "GET" {
			return ErrUnexpectedIBMObjectStoreFragment
		}

		var start, end uint64
		start, err = strconv.ParseUint(fields[4], 10, 64)
		if err != nil {
			return
		}

		end, err = strconv.ParseUint(fields[5], 10, 64)
		if err != nil {
			return
		}

		// No fragment
		if start == 0 && end == rec.Size-1 {
			return
		}

		err = reader.checkFragment(rec)
	}
	return
}

func (reader *IBMObjectStoreReader) checkFragment(rec *Record) error {
	if rec.Start != 0 {
		return ErrIgnoreIBMObjectStoreFragment
	}

	return nil
	// fragment, exist := reader.incompleted[rec.Key]
	// if !exist {
	// 	fragment = &fragmentTracer{
	// 		Key:  rec.Key,
	// 		Size: rec.Size,
	// 		Seen: rec.End - rec.Start + 1,
	// 		Map:  make([]uint64, 2, 100), // Just big enough for most cases.
	// 	}
	// 	fragment.Map[0] = rec.Start
	// 	fragment.Map[1] = rec.End
	// 	reader.incompleted[rec.Key] = fragment
	// 	reader.incompletedSeen++
	// 	return nil
	// }

	// // Check overlap
	// var i int
	// for i = 0; i < len(fragment.Map)/2; i++ {
	// 	if rec.End >= fragment.Map[i] && rec.Start <= fragment.Map[i+1] {
	// 		return ErrUnexpectedIBMObjectStoreOverlappedFragment
	// 	} else if i > 0 && rec.Start == fragment.Map[i-1]+1 && rec.End == fragment.Map[i]-1 {
	// 		// concat
	// 		fragment.Map[i-1] = fragment.Map[i+1]
	// 		fragment.Map = fragment.Map[:i]
	// 		break
	// 	} else if i > 0 && rec.Start == fragment.Map[i-1]+1 {
	// 		// merge to last range
	// 		fragment.Map[i-1] = rec.End
	// 		break
	// 	} else if rec.End == fragment.Map[i]-1 {
	// 		// merge to current range
	// 		fragment.Map[i] = rec.Start
	// 		break
	// 	} else if rec.End < fragment.Map[i] {
	// 		// insert a new range before the current range
	// 		fragment.Map = append(fragment.Map, 0, 0)
	// 		copy(fragment.Map[2:], fragment.Map[:len(fragment.Map)-2])
	// 		fragment.Map[0] = rec.Start
	// 		fragment.Map[1] = rec.End
	// 		break
	// 	}
	// 	// continue to next range
	// }

	// // No overlap, update seen.
	// fragment.Seen += rec.End - rec.Start + 1
	// if fragment.Seen == fragment.Size {
	// 	delete(reader.incompleted, rec.Key)
	// } else if i < len(fragment.Map)/2 {
	// 	// pass
	// } else if rec.Start == fragment.Map[i-1]+1 {
	// 	fragment.Map[i-1] = rec.End
	// } else {
	// 	fragment.Map = append(fragment.Map, rec.Start, rec.End)
	// }
	// return ErrIgnoreIBMObjectStoreFragment
}
