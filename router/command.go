package router

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// CommandType:byte is used for performance
//type CommandType byte
//
//const (
//	commandType_Query CommandType = 0
//	commandType_Join  CommandType = 1
//	commandType_Leave CommandType = 2
//)

// CommandType:string is used for debugging
type CommandType string

const (
	commandType_Query CommandType = "Query"
	commandType_Join  CommandType = "Join"
	commandType_Leave CommandType = "Leave"
)

type command struct {
	CommandType   CommandType
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
