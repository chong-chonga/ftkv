package tool

import (
	"bufio"
	"github.com/pkg/errors"
	"io"
	"io/fs"
	"log"
	"os"
	"runtime"
	"strconv"
)

//
// Storage is to support for Raft and kvdb to save persistent data
// To ensure that the stored raft state and snapshot are consistent, Storage uses os.Rename to rename files atomically
// Storage store the raft state in a file ending in .rf,
// the snapshot and its corresponding raft state is stored in the file at the end of .rfs.
// If an error occurs while reading the raft state or snapshot, the program will be terminated and an error message will be reported.
// After calling the ReadRaftState or ReadSnapshot method, if the data exists, it will be cached;
// The cache will be refreshed when SaveRaftState or SaveSnapshot is called.
// .rfs file format:
// RAFT -- file header
// stateVersion -- version of raft state
// stateSize -- size of raft state
// state(bytes) -- raft state
// snapshot(bytes) -- snapshot of service
//
const fileHeader = "RAFT"

const LogEnabled = true

func logInfo(v ...any) {
	if LogEnabled {
		log.Println(v...)
	}
}

// Storage is not thread safe
type Storage struct {
	nextRaftStateVersion int64
	raftState            []byte
	snapshot             []byte
	raftStatePath        string
	snapshotPath         string
}

type StorageError struct {
	Op     string
	Target string
	Err    error
}

func (s *StorageError) Error() string {
	return s.Op + " " + s.Target + ":" + s.Err.Error()
}

type errWriter struct {
	file *os.File
	e    error
	wr   *bufio.Writer
}

func newErrWriter(file *os.File) *errWriter {
	return &errWriter{
		file: file,
		wr:   bufio.NewWriter(file),
	}
}

func (ew *errWriter) write(p []byte) {
	if ew.e == nil {
		_, ew.e = ew.wr.Write(p)
	}
}

func (ew *errWriter) writeString(s string) {
	if ew.e == nil {
		_, ew.e = ew.wr.WriteString(s)
	}
}

// atomicOverwrite write the buffered data to disk and overwrite the file corresponding to the path
func (ew *errWriter) atomicOverwrite(path string) error {
	err := ew.e
	if err == nil {
		err = ew.wr.Flush()
	}
	if err != nil {
		return err
	}
	err = ew.file.Sync()
	if err != nil {
		return err
	}
	err = ew.file.Close()
	if err != nil {
		return err
	}
	err = os.Rename(ew.file.Name(), path)
	if err != nil {
		// deletion failure will not affect, just ignore
		_ = os.Remove(ew.file.Name())
	}
	return err
}

func MakeStorage(serverNum int) (*Storage, error) {
	s := &Storage{}
	raftStatePath := "raft" + strconv.Itoa(serverNum) + ".rf"
	snapshotPath := "raft" + strconv.Itoa(serverNum) + ".rfs"
	if runtime.GOOS == "windows" {
		raftStatePath = os.TempDir() + "\\" + raftStatePath
		snapshotPath = os.TempDir() + "\\" + snapshotPath
	}
	s.raftStatePath = raftStatePath
	s.snapshotPath = snapshotPath
	raftState, raftStateVersion, snapshot, err := s.initRead()
	if err != nil {
		return nil, err
	}
	s.raftState = raftState
	s.snapshot = snapshot
	if raftStateVersion > 0 {
		s.nextRaftStateVersion = raftStateVersion + 1
	} else {
		s.nextRaftStateVersion = 1
	}
	return s, nil
}

func (ps *Storage) SaveRaftState(state []byte) error {
	tmpFile, err := os.CreateTemp("", "raft*.rf")
	if err != nil {
		return &StorageError{Op: "save", Target: "raft state", Err: err}
	}
	writer := newErrWriter(tmpFile)
	writer.writeString(fileHeader)
	ps.writeRaftState(writer, state)
	err = writer.atomicOverwrite(ps.raftStatePath)
	if err != nil {
		return &StorageError{Op: "save", Target: "raft state", Err: err}
	}
	return nil
}

// SaveStateAndSnapshot save both Raft state and K/V snapshot as a single atomic action
// to keep them consistent.
func (ps *Storage) SaveStateAndSnapshot(state []byte, snapshot []byte) error {
	tmpFile, err := os.CreateTemp("", "raft*.rfs")
	if err != nil {
		return &StorageError{Op: "save", Target: "snapshot", Err: err}
	}
	writer := newErrWriter(tmpFile)
	writer.writeString(fileHeader)
	ps.writeRaftState(writer, state)
	ps.writeSnapshot(writer, snapshot)
	err = writer.atomicOverwrite(ps.snapshotPath)
	if err != nil {
		return &StorageError{Op: "save", Target: "snapshot", Err: err}
	}
	return nil
}

func (ps *Storage) writeRaftState(writer *errWriter, state []byte) {
	writer.writeString(strconv.FormatInt(ps.nextRaftStateVersion, 10) + "\t")
	raftStateSize := len(state)
	writer.writeString(strconv.Itoa(raftStateSize) + "\t")
	if raftStateSize != 0 {
		writer.write(state)
	}
	ps.nextRaftStateVersion++
}

func (ps *Storage) writeSnapshot(writer *errWriter, snapshot []byte) {
	snapshotSize := len(snapshot)
	writer.writeString(strconv.Itoa(snapshotSize) + "\t")
	if snapshotSize != 0 {
		writer.write(snapshot)
	}
}

func (ps *Storage) ReadRaftState() []byte {
	return ps.raftState
}

func (ps *Storage) ReadSnapshot() []byte {
	return ps.snapshot
}

func (ps *Storage) initRead() ([]byte, int64, []byte, error) {
	var raftState1 []byte
	var raftState2 []byte
	var snapshot []byte
	var version1 int64 = -1
	var version2 int64 = -1
	var err error
	raftState1, version1, err = ps.readRF()
	if err != nil {
		return nil, -1, nil, err
	}
	raftState2, version2, snapshot, err = ps.readRFS()
	if err != nil {
		return nil, -1, nil, err
	}
	if version1 > version2 {
		return raftState1, version1, snapshot, nil
	}
	return raftState2, version2, snapshot, nil
}

func (ps *Storage) readRF() ([]byte, int64, error) {
	var raftState []byte
	var version int64 = -1
	file, err := openIfExists(ps.raftStatePath)
	if err != nil {
		return nil, -1, &StorageError{Op: "read", Target: ps.raftStatePath, Err: err}
	}
	if file != nil {
		reader := bufio.NewReader(file)
		err = checkFileHeader(reader)
		if err != nil {
			return nil, -1, &StorageError{Op: "read", Target: ps.raftStatePath, Err: err}
		}
		raftState, version, err = ps.readRaftState(reader)
		if err != nil {
			return nil, -1, &StorageError{Op: "read", Target: ps.raftStatePath, Err: err}
		}
		// close will return an error if it has already been called, ignore
		_ = file.Close()
	}
	return raftState, version, nil
}

func (ps *Storage) readRFS() ([]byte, int64, []byte, error) {
	var raftState []byte
	var version int64 = -1
	var snapshot []byte
	file, err := openIfExists(ps.snapshotPath)
	if err != nil {
		return nil, -1, nil, &StorageError{Op: "read", Target: ps.snapshotPath, Err: err}
	}
	if file != nil {
		reader := bufio.NewReader(file)
		err = checkFileHeader(reader)
		if err != nil {
			return nil, -1, nil, &StorageError{Op: "read", Target: ps.snapshotPath, Err: err}
		}
		raftState, version, err = ps.readRaftState(reader)
		if err != nil {
			return nil, -1, nil, &StorageError{Op: "read", Target: ps.snapshotPath, Err: err}
		}
		snapshot, err = ps.readSnapshot(reader)
		if err != nil {
			return nil, -1, nil, &StorageError{Op: "read", Target: ps.snapshotPath, Err: err}
		}
		// close will return an error if it has already been called, ignore
		_ = file.Close()
	}
	return raftState, version, snapshot, nil
}

func openIfExists(name string) (fs.File, error) {
	file, err := os.Open(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	return file, nil
}

// ErrHeader indicates that the file header is not valid
var ErrHeader = errors.New("invalid file header")

// ErrFormat indicates that the file content is not expected
var ErrFormat = errors.New("invalid file format")

func checkFileHeader(reader *bufio.Reader) error {
	for i := range fileHeader {
		readByte, err := reader.ReadByte()
		if err != nil || readByte != fileHeader[i] {
			return ErrHeader
		}
	}
	return nil
}

func (ps *Storage) readRaftState(reader *bufio.Reader) ([]byte, int64, error) {
	readString, err := reader.ReadString('\t')
	if err != nil || len(readString) < 2 {
		return nil, -1, ErrFormat
	}
	var version int64
	readString = readString[0 : len(readString)-1]
	version, err = strconv.ParseInt(readString, 10, 64)
	if err != nil {
		return nil, -1, ErrFormat
	}
	readString, err = reader.ReadString('\t')
	if err != nil || len(readString) < 2 {
		return nil, -1, ErrFormat
	}
	readString = readString[0 : len(readString)-1]
	var stateSize int
	stateSize, err = strconv.Atoi(readString)
	if err != nil {
		return nil, -1, ErrFormat
	}
	if stateSize <= 0 {
		return nil, version, nil
	}
	readBytes := make([]byte, stateSize)
	_, err = io.ReadFull(reader, readBytes)
	if err != nil {
		return nil, -1, ErrFormat
	}
	return readBytes, version, nil
}

func (ps *Storage) readSnapshot(reader *bufio.Reader) ([]byte, error) {
	readString, err := reader.ReadString('\t')
	if err != nil || len(readString) < 2 {
		return nil, ErrFormat
	}
	readString = readString[0 : len(readString)-1]
	var stateSize int
	stateSize, err = strconv.Atoi(readString)
	if err != nil {
		return nil, ErrFormat
	}
	if stateSize <= 0 {
		return nil, nil
	}
	readBytes := make([]byte, stateSize)
	_, err = io.ReadFull(reader, readBytes)
	if err != nil {
		return nil, ErrFormat
	}
	return readBytes, nil
}
