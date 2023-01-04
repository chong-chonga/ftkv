package storage

import (
	"bufio"
	"errors"
	"io"
	"io/fs"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
)

//
// Storage is to support for Raft and kvdb to save persistent data
// To ensure that the stored raft state and snapshot are consistent, Storage uses os.Rename to rename files atomically
// Storage store the raft state in a file ending in .rf,
// the snapshot and its corresponding raft state is stored in the file at the end of .rfs.
// If an error occurs while reading the raft state or snapshot, the program will be terminated and an error message will be reported.
// After calling the ReadRaftState or ReadSnapshot method, if the data exists, it will be cached;
// The cache will be refreshed when SaveRaftState or SaveSnapshot is called.
//
const fileHeader = "RAFT"

type Storage struct {
	mu                   sync.Mutex
	nextRaftStateVersion int64
	raftState            []byte
	snapshot             []byte
	raftStatePath        string
	snapshotPath         string
}

func MakeStorage(serverNum int) *Storage {
	s := &Storage{}
	raftStatePath := "raft" + strconv.Itoa(serverNum) + ".rf"
	snapshotPath := "raft" + strconv.Itoa(serverNum) + ".rfs"
	if runtime.GOOS == "windows" {
		raftStatePath = os.TempDir() + "\\" + raftStatePath
		snapshotPath = os.TempDir() + "\\" + snapshotPath
	}
	s.raftStatePath = raftStatePath
	s.snapshotPath = snapshotPath
	return s
}

func clone(orig []byte) []byte {
	if nil == orig {
		return nil
	}
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Storage) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftState = clone(state)
	tmpFile, err := os.CreateTemp("", "raft*.rf")
	if err != nil {
		log.Fatalln("create temp file for persistence failed! errorInfo: ", err)
	}
	writer := bufio.NewWriter(tmpFile)
	_, err = writer.WriteString(fileHeader)
	if err != nil {
		log.Fatalln("error occurs during writing raft state! errorInfo: write file header failed!")
	}
	ps.writeRaftState(writer, state)
	writer.Flush()
	tmpFile.Close()
	err = os.Rename(tmpFile.Name(), ps.raftStatePath)
	if err != nil {
		os.Remove(tmpFile.Name())
		log.Fatalln("save raft state file failed! errorInfo: ", err)
	}
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Storage) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftState = clone(state)
	ps.snapshot = clone(snapshot)
	tmpFile, err := os.CreateTemp("", "raft*.rfs")
	if err != nil {
		log.Fatalln("create temp file for persistence failed! errorInfo: ", err)
	}
	writer := bufio.NewWriter(tmpFile)
	_, err = writer.WriteString(fileHeader)
	if err != nil {
		log.Fatalln("error occurs during writing raft state! errorInfo: write file header failed!")
	}
	ps.writeRaftState(writer, state)
	if ps.snapshot != nil && len(ps.snapshot) > 0 {
		writer.Write(ps.snapshot)
	}
	writer.Flush()
	tmpFile.Close()
	//temp.Sync()
	err = os.Rename(tmpFile.Name(), ps.snapshotPath)
	if err != nil {
		os.Remove(tmpFile.Name())
		log.Fatalln("save snapshot file failed! errorInfo: ", err)
	}
}

func (ps *Storage) writeRaftState(writer *bufio.Writer, state []byte) {
	writer.WriteString(strconv.FormatInt(ps.nextRaftStateVersion, 10) + "\t")
	raftStateSize := len(state)
	writer.WriteString(strconv.Itoa(raftStateSize) + "\t")
	writer.Write(state)
	ps.nextRaftStateVersion++
}

func (ps *Storage) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.raftState == nil {
		raftState, nextRaftStateVersion, snapshot := ps.readRFAndRFS()
		ps.raftState = raftState
		ps.nextRaftStateVersion = nextRaftStateVersion
		ps.snapshot = snapshot
	}
	return clone(ps.raftState)
}

func (ps *Storage) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.snapshot == nil {
		_, _, snapshot := ps.readRFS()
		ps.snapshot = snapshot
	}
	return clone(ps.snapshot)
}

func (ps *Storage) readRFAndRFS() ([]byte, int64, []byte) {
	var raftState1 []byte
	var raftState2 []byte
	var snapshot []byte
	var version1 int64 = -1
	var version2 int64 = -1
	file := safeOpen(ps.raftStatePath)
	if file != nil {
		reader := bufio.NewReader(file)
		readFileHeader(reader)
		raftState1, version1, _ = ps.readRaftState(reader)
		file.Close()
	}
	raftState2, version2, snapshot = ps.readRFS()
	if version1 == -1 && version2 == -1 {
		ps.nextRaftStateVersion = 1
	} else if version1 > version2 {
		return raftState1, version1, snapshot
	}
	return raftState2, version2, snapshot
}

func (ps *Storage) readRFS() ([]byte, int64, []byte) {
	var raftState []byte
	var version int64 = -1
	var snapshot []byte
	file := safeOpen(ps.snapshotPath)
	if file != nil {
		reader := bufio.NewReader(file)
		readLen1 := int64(readFileHeader(reader))
		var readLen2 int64
		raftState, version, readLen2 = ps.readRaftState(reader)
		stat, err := file.Stat()
		if err != nil {
			log.Fatalln("error getting file information, errorInfo:", err)
		}
		ps.snapshot = ps.readSnapshot(reader, stat.Size()-readLen1-readLen2)
		file.Close()
	}
	return raftState, version, snapshot
}

func safeOpen(name string) fs.File {
	file, err := os.Open(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		log.Fatalln("cannot open file:[", file, "], please check you have permission to access this file! errorInfo: ", err)
	}
	return file
}

func readFileHeader(reader *bufio.Reader) int {
	for i := range fileHeader {
		readByte, _ := reader.ReadByte()
		if readByte != fileHeader[i] {
			log.Fatalln("unknown file type! please check the file or delete the file! errorInfo: file header is wrong!")
		}
	}
	return len(fileHeader)
}

func (ps *Storage) readRaftState(reader *bufio.Reader) ([]byte, int64, int64) {
	var readLen int64 = 0
	readString, err := reader.ReadString('\t')
	if err != nil || len(readString) < 2 {
		log.Fatalln("unknown file type! please check the file or delete the file! errorInfo: expect to read version number but not exists!")
	}
	readLen += int64(len(readString))
	readString = readString[0 : len(readString)-1]
	var version int64
	version, err = strconv.ParseInt(readString, 10, 64)
	if err != nil {
		log.Fatalln("unknown file type! please check the file or delete the file! errorInfo: expect to read version number but wrong format!")
	}
	readString, err = reader.ReadString('\t')
	if err != nil || len(readString) < 2 {
		log.Fatalln("unknown file type! please check the file or delete the file! errorInfo: expect to read version number but not exists!")
	}
	readLen += int64(len(readString))
	readString = readString[0 : len(readString)-1]
	var stateSize int
	stateSize, err = strconv.Atoi(readString)
	if err != nil {
		log.Fatalln("unknown file type! please check the file or delete the file! errorInfo: expect to read state size but wrong format!")
	}
	readBytes := make([]byte, stateSize)
	read, _ := io.ReadFull(reader, readBytes)
	if read != stateSize {
		log.Fatalln("raft state is not fully read!")
	}
	readLen += int64(stateSize)
	return readBytes, version, readLen
}

func (ps *Storage) readSnapshot(reader *bufio.Reader, remain int64) []byte {
	s := make([]byte, remain)
	_, err := io.ReadFull(reader, s)
	if err != nil {
		log.Fatalln("read snapshot failed! errorInfo: ", err)
	}
	_, err = reader.ReadByte()
	if err == nil {
		log.Println("[warning] file is not fully read!")
	}
	return s
}
