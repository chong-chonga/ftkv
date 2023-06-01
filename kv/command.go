package kv

import (
	"bytes"
	"encoding/json"
	"fmt"
)

//// commandType:byte is used for performance
//type commandType byte
//
//const (
//	commandType_Get             commandType = 1
//	commandType_Set             commandType = 2
//	commandType_Delete          commandType = 3
//	commandType_Configure       commandType = 4
//	commandType_Transfer_Shards commandType = 5
//	commandType_Delete_Shards   commandType = 6
//)

// commandType:string is used for debugging
type commandType string

const (
	commandType_Get             commandType = "Get"
	commandType_Set             commandType = "Set"
	commandType_Delete          commandType = "Delete"
	commandType_Configure       commandType = "Configure"
	commandType_Transfer_Shards commandType = "ShardsTransfer"
	commandType_Delete_Shards   commandType = "ShardsDelete"
)

type command struct {
	CommandType commandType

	// used to get/set/delete
	Key   string
	Value string

	// used to configure/shards transfer/shards delete
	ConfigId int32

	// used to configure
	ShardsToGet      map[int]byte       // shard -> 1
	ShardsToTransfer map[int32][]int    // groupId -> shards
	ReplicaGroups    map[int32][]string // groupId -> server addresses

	// used to shards transfer
	Shards []int32           // shards
	Data   map[string]string // key -> value

	// used to shards delete
	GroupId int32
	Keys    []string
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
