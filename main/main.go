package main

import (
	"fmt"
	"github.com/ftkv/v1/tool"
	"log"
)

// import (
//
//	_ "embed"
//	"fmt"
//	"github.com.chongchonga/kvservice/v1/kvclient"
//	"github.com.chongchonga/kvservice/v1/kvserver"
//	"log"
//	"time"
//
// )
//
// //go:embed kvservice1.yaml
// var data1 []byte
//
// //go:embed kvservice2.yaml
// var data2 []byte
//
// //go:embed kvservice3.yaml
// var data3 []byte
//
// //go:embed kvclient.yaml
// var data4 []byte
func main() {
	targetSuffix := ".txt"
	dir := "C:\\baidudownload"
	files, err := tool.FindFiles(dir, targetSuffix)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("find %d files\n", len(files))
	var httpsLinks []string
	for _, file := range files {
		link, err := tool.FindBaiduNetDiskLinkIn(file)
		if err != nil {
			log.Fatalln(err)
		}
		httpsLinks = append(httpsLinks, tool.ExtractHTTPSLinks(link)...)
	}
	for _, link := range httpsLinks {
		fmt.Println(link)
	}

	//for _, file := range files {
	//	err := os.Remove(file)
	//	if err != nil {
	//		fmt.Printf("remove %s fail, err: %s\n", file, err)
	//	} else {
	//		fmt.Printf("remove %s success\n", file)
	//	}
	//}
	//fmt.Println(files)
	//fmt.Printf("total %d files\n", len(files))
	//err = tool.ReplaceFileNames(files, targetSuffix, replacement)
	//if err != nil {
	//	log.Fatalln(err)
	//}
	//	_, err := kvserver.StartKVServer(data1)
	//	if err != nil {
	//		log.Fatalln(err)
	//	}
	//	_, err = kvserver.StartKVServer(data2)
	//	if err != nil {
	//		log.Fatalln(err)
	//	}
	//	_, err = kvserver.StartKVServer(data3)
	//	if err != nil {
	//		log.Fatalln(err)
	//	}
	//	time.Sleep(5 * time.Second)
	//	client, err := kvclient.NewClient(data4)
	//	if err != nil {
	//		panic(err.Error())
	//	}
	//	for {
	//		fmt.Println("-------------menu----------------")
	//		fmt.Println("1. get 2. put 3. append 4. delete")
	//		fmt.Println("please enter the operation you want to perform: ")
	//		var choice int
	//		_, err = fmt.Scan(&choice)
	//		for err != nil || choice > 4 || choice < 1 {
	//			fmt.Println("please enter the correct operation : ")
	//			_, err = fmt.Scan(&choice)
	//		}
	//		var v string
	//		switch choice {
	//		case 1:
	//			var key string
	//			var exist bool
	//			fmt.Print("please enter the key you wanna to get: ")
	//			fmt.Scan(&key)
	//			v, exist, err = client.Get(key)
	//			if err == nil {
	//				if !exist {
	//					fmt.Print("key:", key, " not exists, ")
	//				} else {
	//					fmt.Print("the value for key ", key, " is ", v, ", ")
	//				}
	//			}
	//			break
	//		case 2, 3:
	//			var key string
	//			var value string
	//			fmt.Print("please enter the key&value: ")
	//			fmt.Scan(&key, &value)
	//			if choice == 2 {
	//				err = client.Put(key, value)
	//			} else {
	//				err = client.Append(key, value)
	//			}
	//			break
	//		case 4:
	//			var key string
	//			fmt.Print("please enter the key you wanna to delete: ")
	//			fmt.Scan(&key)
	//			err = client.Delete(key)
	//		}
	//		if err != nil {
	//			fmt.Println("errInfo:", err)
	//		} else {
	//			fmt.Println("the command was executed successfully")
	//		}
	//	}
}
