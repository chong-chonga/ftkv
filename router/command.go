package router

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// commandType:byte is used for performance
//type commandType byte
//
//const (
//	commandType_Query commandType = 0
//	commandType_Join  commandType = 1
//	commandType_Leave commandType = 2
//)

// commandType:string is used for debugging
type commandType string

const (
	commandType_Query commandType = "Query"
	commandType_Join  commandType = "Join"
	commandType_Leave commandType = "Leave"
)

type command struct {
	CommandType   commandType
	ConfigId      int32
	ReplicaGroups map[int32][]string
	GroupIds      []int32
}

func (command *command) CommandString() string {
	buffer := new(bytes.Buffer)
	encoder := json.NewEncoder(buffer)
	err := encoder.Encode(command)
	if err == nil {
		return buffer.String()
	}
	return fmt.Sprintf("%v", command)
}
