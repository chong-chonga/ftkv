package main

import (
	"fmt"
	"kvraft/kvclient"
	"kvraft/kvserver"
	"kvraft/tool"
	"log"
	"strconv"
	"time"
)

const (
	servers    = 3
	serverPort = 8080
	raftPort   = 8090
)

func getServerAddresses() []string {
	addrs := make([]string, servers)
	port := serverPort
	for i := 0; i < servers; i++ {
		addr := "localhost:" + strconv.Itoa(port)
		addrs[i] = addr
		port++
	}
	return addrs
}

func getRaftAddresses(me int) []string {
	addrs := make([]string, servers-1)
	j := 0
	for i := 0; i < servers; i++ {
		if i == me {
			continue
		}
		addrs[j] = "localhost:" + strconv.Itoa(raftPort+i)
		j++
	}
	return addrs
}

func main() {
	me := 0
	for ; me < 3; me++ {
		s1, err := tool.MakeStorage(me)
		if err != nil {
			log.Fatalln(err)
		}
		_, err = kvserver.StartKVServer(serverPort+me, me, getRaftAddresses(me), raftPort+me, s1, 30)
		if err != nil {
			log.Fatalln(err)
		}
	}
	time.Sleep(5 * time.Second)
	client, err := kvclient.NewClient(getServerAddresses())
	if err != nil {
		panic(err.Error())
	}
	for {
		fmt.Println("-------------menu----------------")
		fmt.Println("1. get 2. put 3. append 4. delete")
		fmt.Println("please enter the operation you want to perform: ")
		var choice int
		_, err = fmt.Scan(&choice)
		for err != nil || choice > 4 || choice < 1 {
			fmt.Println("please enter the correct operation : ")
			_, err = fmt.Scan(&choice)
		}
		var v string
		switch choice {
		case 1:
			var key string
			var exist bool
			fmt.Print("please enter the key you wanna to get: ")
			fmt.Scan(&key)
			v, exist, err = client.Get(key)
			if err == nil {
				if !exist {
					fmt.Print("key:", key, " not exists, ")
				} else {
					fmt.Print("the value for key ", key, " is ", v, ", ")
				}
			}
			break
		case 2, 3:
			var key string
			var value string
			fmt.Print("please enter the key&value: ")
			fmt.Scan(&key, &value)
			if choice == 2 {
				err = client.Put(key, value)
			} else {
				err = client.Append(key, value)
			}
			break
		case 4:
			var key string
			fmt.Print("please enter the key you wanna to delete: ")
			fmt.Scan(&key)
			err = client.Delete(key)
		}
		if err != nil {
			fmt.Println("errInfo:", err)
		} else {
			fmt.Println("the command was executed successfully")
		}
	}
}
