package main

import (
	"kvraft/src/grpc/client"
	"kvraft/src/grpc/server"
	"kvraft/src/kvdb"
	"kvraft/src/storage"
	"strconv"
	"testing"
	"time"
)

const (
	servers  = 1
	nextPort = 8080
)

func getServerAddresses() []string {
	addrs := make([]string, servers)
	for i := 0; i < servers; i++ {
		port := nextPort + i
		addr := "localhost:" + strconv.Itoa(port)
		addrs[i] = addr
	}
	return addrs
}

func TestServer1(t *testing.T) {
	server.StartKVServer(getServerAddresses(), 0, storage.MakeStorage(0), 500)
	select {}
}

func TestServer2(t *testing.T) {
	server.StartKVServer(getServerAddresses(), 1, storage.MakeStorage(1), 500)
	select {}
}

func TestServer3(t *testing.T) {
	server.StartKVServer(getServerAddresses(), 2, storage.MakeStorage(2), 500)
	select {}
}

func TestBasicKVServers(t *testing.T) {
	clerk := client.MakeClerk(getServerAddresses())
	now := time.Now()
	clerk.Append("1", "1")
	s := clerk.Get("1")
	println(s)
	println(time.Since(now).String())
}

func TestBasicKVServers2(t *testing.T) {
	for i := 0; i < 100; i++ {
		//go func() {
		for j := 0; j < 10; j++ {
			kvdb.MakeClerk(getServerAddresses())
		}
		//}()
	}
}
func TestBasicKVServers3(t *testing.T) {
	for i := 0; i < 100; i++ {
		//go func() {
		for j := 0; j < 10; j++ {
			kvdb.MakeClerk(getServerAddresses())
		}
		//}()
	}
}

const fileHeader = "RAFT"
const state = "ushg"
const snapshot = "oklakfgopkiqwoperiqw qwkdlaks123"

func TestSaveRaftStateAndSnapshot(t *testing.T) {

	m := make(map[int]int)
	m[1] = 2
	m[2] = 3
	for k, v := range m {
		println(k)
		println(v)
	}
}
