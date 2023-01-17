package main

import (
	"kvraft/server"
	"kvraft/tool/v1"
	"log"
	"strconv"
	"testing"
)

func getRaftAddresses() []string {
	addrs := make([]string, servers)
	port := servers + nextPort
	for i := 0; i < servers; i++ {
		addr := "localhost:" + strconv.Itoa(port)
		addrs[i] = addr
		port++
	}
	return addrs
}

func TestServer1(t *testing.T) {
	s1, err := storage.MakeStorage(0)
	if err != nil {
		log.Fatalln(err)
	}
	server.StartKVServer(getServerAddresses(), getRaftAddresses(), 0, s1, 40, "")
	select {}
}

func TestServer2(t *testing.T) {
	s2, err := storage.MakeStorage(1)
	if err != nil {
		log.Fatalln(err)
	}
	server.StartKVServer(getServerAddresses(), getRaftAddresses(), 1, s2, 40, "")
	select {}
}

func TestServer3(t *testing.T) {
	s3, err := storage.MakeStorage(2)
	if err != nil {
		log.Fatalln(err)
	}
	server.StartKVServer(getServerAddresses(), getRaftAddresses(), 2, s3, 40, "")
	select {}
}
