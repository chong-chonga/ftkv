package raft

import "github.com/kvservice/v1/common"

type Op struct {
	RequestType common.RequestType
	Key         string
	Value       string
	UUID        string
}
