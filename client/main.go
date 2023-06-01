package main

import (
	_ "embed"
	"fmt"
	"github.com/ftkv/v1/kv"
	"github.com/ftkv/v1/router"
	"log"
	"strings"
	"time"
)

//go:embed router_client.yaml
var routerClientConfig []byte

//go:embed kv_client.yaml
var kvClientConfig []byte

func main() {
	routerClient, err := router.MakeClient(routerClientConfig)
	if err != nil {
		log.Fatalln(err)
	}
	kvclient, err := kv.MakeClient(kvClientConfig)
	if err != nil {
		log.Fatalln(err)
	}

	for {
		var command string
		time.Sleep(20 * time.Millisecond)
		fmt.Print("$: ")
		fmt.Scan(&command)
		command = strings.ToLower(command)
		var e error
		var key string
		var value string
		var exist bool
		switch command {
		case "get":
			fmt.Scan(&key)
			value, exist, e = kvclient.Get(key)
			if e == nil {
				fmt.Printf("value=%v, exist=%v\n", value, exist)
			}
			break
		case "set":
			fmt.Scan(&key)
			fmt.Scan(&value)
			e = kvclient.Set(key, value)
			if e == nil {
				fmt.Println("ok")
			}
			break
		case "delete":
			fmt.Scan(&key)
			e = kvclient.Delete(key)
			if e == nil {
				fmt.Println("ok")
			}
			break
		case "join":
			var gid int32
			fmt.Scan(&gid)
			var serverAddresses []string
			for {
				var address string
				fmt.Scan(&address)
				if address == "#" {
					break
				}
				serverAddresses = append(serverAddresses, address)
			}
			m := make(map[int32][]string)
			m[gid] = serverAddresses
			e = routerClient.Join(m)
			if e == nil {
				fmt.Println("ok")
			}
			break
		case "leave":
			var gid int
			fmt.Scan(&gid)
			e = routerClient.Leave([]int32{int32(gid)})
			if e == nil {
				fmt.Println("ok")
			}
			break
		case "exit":
			return
		}
		if e != nil {
			fmt.Println(e)
		}
	}
}
