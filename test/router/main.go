package main

import (
	_ "embed"
	"github.com/ftkv/v1/router"
	"log"
)

//go:embed router1.yaml
var router1Config []byte

//go:embed router2.yaml
var router2Config []byte

//go:embed router3.yaml
var router3Config []byte

func main() {
	_, err := router.StartServer(router1Config)
	if err != nil {
		log.Fatalln(err)
	}

	_, err = router.StartServer(router2Config)
	if err != nil {
		log.Fatalln(err)
	}

	_, err = router.StartServer(router3Config)
	if err != nil {
		log.Fatalln(err)
	}
	select {}
}
