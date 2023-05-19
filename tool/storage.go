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
// Storage is used to read and persist raft state and snapshot of service.
// Create a storage by passing the service name to the MakeStorage method, the service name will be part of the persistence file name.
// If you want to create Storage for multiple services, please specify a unique name for each service.
// To ensure that the stored raft state and snapshot are consistent, Storage uses os.Rename to rename files atomically
// Storage store the raft state in a file ending in .rf,
// the snapshot and its corresponding raft state is stored in the file at the end of .rfs.
// After calling the GetRaftState or GetSnapshot method, if the data exists, it will be cached;
// The cache will be refreshed when SaveRaftState or SaveSnapshot is called.
// .rfs file format:
// RAFT -- file header
// stateVersion -- version of raft state
// stateSize -- size of raft state
// state(bytes) -- raft state
// snapshotSize -- size of snapshot
// snapshot(bytes) -- snapshot of service
// .rf file format
// RAFT -- file header
// stateVersion -- version of raft state
// stateSize -- size of raft state
// state(bytes) -- raft state
// hint: The MakeStorage method will perform an initial read to determine the version number used in the next persistence raft state.
// The version number will increase each time persistence of the raft state.

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

const fileHeader = "RAFT"

// Storage is not thread safe
type Storage struct {
	raftState            []byte
	snapshot             []byte
	nextRaftStateVersion int64
	raftStatePath        string
	snapshotPath         string
}

func MakeStorage(serviceName string) (*Storage, error) {
	s := &Storage{}
	raftStatePath := serviceName + ".rf"
	snapshotPath := serviceName + ".rfs"
	if runtime.GOOS == "windows" {
		raftStatePath = os.TempDir() + "\\" + raftStatePath
		snapshotPath = os.TempDir() + "\\" + snapshotPath
	}
	s.raftStatePath = raftStatePath
	s.snapshotPath = snapshotPath
	log.Printf("service:%s storage: raft state is stored in %s, snapshot and raft state is stored in %s", serviceName, raftStatePath, snapshotPath)
	err := s.initRead()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Storage) initRead() error {
	var raftState1 []byte
	var raftState2 []byte
	var snapshot []byte
	var version1 int64
	var version2 int64
	var err error
	raftState1, version1, err = s.readRaftStateFile()
	if err != nil {
		return err
	}
	raftState2, version2, snapshot, err = s.readSnapshotFile()
	if err != nil {
		return err
	}
	if version1 > version2 {
		s.nextRaftStateVersion = version1 + 1
		s.raftState = raftState1
	} else {
		s.nextRaftStateVersion = version2 + 1
		s.raftState = raftState2
	}
	s.snapshot = snapshot
	return nil
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

func (s *Storage) readRaftStateFile() ([]byte, int64, error) {
	var raftState []byte
	var version int64 = 0
	file, err := openIfExists(s.raftStatePath)
	if err != nil {
		return nil, -1, &StorageError{Op: "read", Target: "raft state", Err: err}
	}
	if file != nil {
		reader := bufio.NewReader(file)
		err = checkFileHeader(reader)
		if err != nil {
			return nil, -1, &StorageError{Op: "read", Target: "raft state", Err: err}
		}
		raftState, version, err = s.readRaftState(reader)
		if err != nil {
			return nil, -1, &StorageError{Op: "read", Target: "raft state", Err: err}
		}
		// close will return an error if it has already been called, ignore
		_ = file.Close()
	}
	return raftState, version, nil
}

func (s *Storage) readSnapshotFile() ([]byte, int64, []byte, error) {
	var raftState []byte
	var version int64 = 0
	var snapshot []byte
	file, err := openIfExists(s.snapshotPath)
	if err != nil {
		return nil, -1, nil, &StorageError{Op: "read", Target: "raft state and snapshot", Err: err}
	}
	if file != nil {
		reader := bufio.NewReader(file)
		err = checkFileHeader(reader)
		if err != nil {
			return nil, -1, nil, &StorageError{Op: "read", Target: "raft state and snapshot", Err: err}
		}
		raftState, version, err = s.readRaftState(reader)
		if err != nil {
			return nil, -1, nil, &StorageError{Op: "read", Target: "raft state and snapshot", Err: err}
		}
		snapshot, err = s.readSnapshot(reader)
		if err != nil {
			return nil, -1, nil, &StorageError{Op: "read", Target: "raft state and snapshot", Err: err}
		}
		// close will return an error if it has already been called, ignore
		_ = file.Close()
	}
	return raftState, version, snapshot, nil
}

func (s *Storage) readRaftState(reader *bufio.Reader) ([]byte, int64, error) {
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
	var readBytes []byte
	if stateSize > 0 {
		readBytes = make([]byte, stateSize)
		_, err = io.ReadFull(reader, readBytes)
		if err != nil {
			return nil, -1, ErrFormat
		}
	}
	return readBytes, version, nil
}

func (s *Storage) readSnapshot(reader *bufio.Reader) ([]byte, error) {
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
	var readBytes []byte
	if stateSize > 0 {
		readBytes = make([]byte, stateSize)
		_, err = io.ReadFull(reader, readBytes)
		if err != nil {
			return nil, ErrFormat
		}
	}
	return readBytes, nil
}

// flushAndRename write the buffered data to disk and overwrite the file corresponding to the path
func (ew *errWriter) flushAndRename(path string) error {
	err := ew.e
	if err != nil {
		return err
	}
	err = ew.wr.Flush()
	if err != nil {
		return err
	}
	err = ew.file.Sync()
	if err != nil {
		return err
	}
	// close will return an error if it has already been called, ignore
	_ = ew.file.Close()
	err = os.Rename(ew.file.Name(), path)
	if err != nil {
		// deletion failure will not affect, just ignore
		e := os.Remove(ew.file.Name())
		if e != nil {
			log.Printf("fail to delete temp file:%s, err=%v", ew.file.Name(), e)
		}
	}
	return err
}

func (s *Storage) SaveRaftState(state []byte) error {
	tmpFile, err := os.CreateTemp("", "raft*.rf")
	if err != nil {
		err = &StorageError{Op: "save", Target: "raft state", Err: err}
		return err
	}
	writer := newErrWriter(tmpFile)
	writer.writeString(fileHeader)
	s.writeRaftState(writer, state)
	err = writer.flushAndRename(s.raftStatePath)
	if err != nil {
		err = &StorageError{Op: "save", Target: "raft state", Err: err}
		return err
	}
	s.raftState = clone(state)
	return nil
}

// SaveStateAndSnapshot save both Raft state and K/V snapshot as a single atomic action
// to keep them consistent.
func (s *Storage) SaveStateAndSnapshot(state []byte, snapshot []byte) error {
	tmpFile, err := os.CreateTemp("", "raft*.rfs")
	if err != nil {
		err = &StorageError{Op: "save", Target: "raft state and snapshot", Err: err}
		return err
	}
	writer := newErrWriter(tmpFile)
	writer.writeString(fileHeader)
	s.writeRaftState(writer, state)
	s.writeSnapshot(writer, snapshot)
	err = writer.flushAndRename(s.snapshotPath)
	if err != nil {
		err = &StorageError{Op: "save", Target: "raft state and snapshot", Err: err}
		return err
	}
	s.raftState = clone(state)
	s.snapshot = clone(snapshot)
	return nil
}

func (s *Storage) writeRaftState(writer *errWriter, state []byte) {
	writer.writeString(strconv.FormatInt(s.nextRaftStateVersion, 10) + "\t")
	raftStateSize := len(state)
	writer.writeString(strconv.Itoa(raftStateSize) + "\t")
	if raftStateSize > 0 {
		writer.write(state)
	}
	s.nextRaftStateVersion++
}

func (s *Storage) writeSnapshot(writer *errWriter, snapshot []byte) {
	snapshotSize := len(snapshot)
	writer.writeString(strconv.Itoa(snapshotSize) + "\t")
	if snapshotSize > 0 {
		writer.write(snapshot)
	}
}

func clone(data []byte) []byte {
	d := make([]byte, len(data))
	copy(d, data)
	return d
}

func (s *Storage) GetRaftState() []byte {
	return s.raftState
}

func (s *Storage) GetSnapshot() []byte {
	return s.snapshot
}

func (s *Storage) RaftStateSize() int {
	return len(s.raftState)
}

// ErrHeader indicates that the file header is not valid
var ErrHeader = errors.New("file header is invalid")

// ErrFormat indicates that the file content is not expected
var ErrFormat = errors.New("file format is invalid")

func checkFileHeader(reader *bufio.Reader) error {
	for i := range fileHeader {
		readByte, err := reader.ReadByte()
		if err != nil || readByte != fileHeader[i] {
			return ErrHeader
		}
	}
	return nil
}

// Reset delete storage file(if exists) for the specified service
func Reset(serviceName string) error {
	log.Printf("warning: delete storage file for service:%s", serviceName)
	raftStatePath := serviceName + ".rf"
	snapshotPath := serviceName + ".rfs"
	if runtime.GOOS == "windows" {
		raftStatePath = os.TempDir() + "\\" + raftStatePath
		snapshotPath = os.TempDir() + "\\" + snapshotPath
	}
	err := os.Remove(raftStatePath)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	err = os.Remove(snapshotPath)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}
