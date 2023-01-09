package main

import (
	"fmt"
	"kvraft/src/grpc/client"
	"log"
	"strconv"
)

const (
	servers  = 3
	nextPort = 8080
)

func getServerAddresses() []string {
	addrs := make([]string, servers)
	port := nextPort
	for i := 0; i < servers; i++ {
		addr := "localhost:" + strconv.Itoa(port)
		addrs[i] = addr
		port++
	}
	return addrs
}

func main() {
	client := client.DailClient(getServerAddresses())
	for {
		fmt.Println("-------------menu----------------")
		fmt.Println("1. get 2. put 3. append 4. delete")
		fmt.Println("please enter the operation you want to perform: ")
		var op int
		_, err := fmt.Scan(&op)
		for err != nil || op > 4 || op < 1 {
			fmt.Println("please enter the correct operation : ")
			_, err = fmt.Scan(&op)
		}
		var v string
		switch op {
		case 1:
			var key string
			fmt.Println("please enter the key you wanna to get: ")
			fmt.Scan(&key)
			v, err = client.Get(key)
			if err == nil {
				fmt.Println("the value for key ", key, " is ", v)
			}
			break
		case 2, 3:
			var key string
			var value string
			fmt.Println("please enter the key&value: ")
			fmt.Scan(&key, &value)
			if op == 2 {
				err = client.Put(key, value)
			} else {
				err = client.Append(key, value)
			}
			break
		case 4:
			var key string
			fmt.Println("please enter the key you wanna to delete: ")
			fmt.Scan(&key)
			err = client.Delete(key)
		}
		if err != nil {
			log.Println("errInfo: ", err)
		}
	}
}
