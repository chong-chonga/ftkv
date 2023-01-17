package storage

import (
	"bufio"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
)

//
// support for Raft and kvdb to save persistent
// Raft state (log &c) and k/v server snapshots.
//

type Storage struct {
	mu            sync.Mutex
	raftStateSize int
	raftstate     []byte
	snapshot      []byte
	path          string
}

const fileHeader = "RAFT"

func MakeStorage(server int) *Storage {
	s := &Storage{}
	path := "raft" + strconv.Itoa(server) + ".rf"
	if runtime.GOOS == "windows" {
		path = os.TempDir() + "\\" + path
	}
	s.path = path
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

func (ps *Storage) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.raftStateSize
}

//func (ps *Storage) SaveRaftState(state []byte) {
//	ps.mu.Lock()
//	defer ps.mu.Unlock()
//	ps.raftstate = clone(state)
//}
//
//// Save both Raft state and K/V snapshot as a single atomic action,
//// to help avoid them getting out of sync.
//func (ps *Storage) SaveStateAndSnapshot(state []byte, snapshot []byte) {
//	ps.mu.Lock()
//	defer ps.mu.Unlock()
//	ps.raftstate = clone(state)
//	ps.snapshot = clone(snapshot)
//}
//
//func (ps *Storage) ReadRaftState() []byte {
//	ps.mu.Lock()
//	defer ps.mu.Unlock()
//	return clone(ps.raftstate)
//}
//
//func (ps *Storage) ReadSnapshot() []byte {
//	ps.mu.Lock()
//	defer ps.mu.Unlock()
//	return clone(ps.snapshot)
//}

func (ps *Storage) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}

func (ps *Storage) SaveRaftState(state []byte) {
	ps.mu.Lock()
	snapshot := ps.snapshot
	ps.mu.Unlock()
	ps.SaveStateAndSnapshot(state, snapshot)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Storage) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(state)
	ps.snapshot = clone(snapshot)
	ps.raftStateSize = len(state)
	temp, err := os.CreateTemp("", "raft*.rf")
	if err != nil {
		log.Fatalln(err)
	}
	writer := bufio.NewWriter(temp)
	stateSize := len(state)
	writer.WriteString(fileHeader)
	writer.WriteString(strconv.Itoa(stateSize) + "\n")
	writer.Write(state)
	if ps.snapshot != nil && len(ps.snapshot) > 0 {
		writer.Write(ps.snapshot)
	}
	writer.Flush()
	temp.Close()
	//temp.Sync()
	err = os.Rename(temp.Name(), ps.path)
	if err != nil {
		os.Remove(temp.Name())
		log.Fatalln(err)
	}
}

func (ps *Storage) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.raftstate == nil {
		ps.read()
	}
	return clone(ps.raftstate)
}

func (ps *Storage) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.snapshot == nil {
		ps.read()
	}
	return clone(ps.snapshot)
}

func (ps *Storage) read() {
	file, err := os.Open(ps.path)
	if err != nil {
		log.Println(err)
		return
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	fileHeaderSize := readFileHeader(reader)
	raftState, raftStateSize := readRaftState(reader)
	stat, _ := file.Stat()
	ps.raftstate = raftState
	ps.snapshot = readSnapshot(reader, stat.Size()-fileHeaderSize-raftStateSize)
}

func readFileHeader(reader *bufio.Reader) int64 {
	for i := range fileHeader {
		readByte, _ := reader.ReadByte()
		if readByte != fileHeader[i] {
			log.Fatalf("unknown file type! expected read %c, but read %c", fileHeader[i], readByte)
		}
	}
	return int64(len(fileHeader))
}

func readRaftState(reader *bufio.Reader) ([]byte, int64) {
	readString, err := reader.ReadString('\n')
	if err != nil || len(readString) < 2 {
		log.Fatalln("expect to read number, but not exists!")
	}
	readString = readString[0 : len(readString)-1]
	stateSize, err := strconv.Atoi(readString)
	if err != nil {
		log.Fatalln("expect to read a number, but wrong format!")
	}
	readBytes := make([]byte, stateSize)
	read, _ := io.ReadFull(reader, readBytes)
	if read != stateSize {
		log.Fatalln("raft state is not fully read!")
	}
	return readBytes, int64(len(readString)) + 1 + int64(read)
}

func readSnapshot(reader *bufio.Reader, reamain int64) []byte {
	s := make([]byte, reamain)
	io.ReadFull(reader, s)
	_, err := reader.ReadByte()
	if err == nil {
		log.Fatalln("snapshot is not fully read!")
	}
	return s
}
