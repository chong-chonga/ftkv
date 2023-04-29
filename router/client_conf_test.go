package router

import (
	_ "embed"
	"fmt"
	"log"
	"testing"
)

//go:embed client_conf.yaml
var clientConfig []byte

func TestReadClientConf(t *testing.T) {
	fmt.Println("Test read client config")
	config, err := readClientConfig(clientConfig)
	if err != nil {
		log.Fatalln(err)
	}
	if config.Timeout == 0 {
		log.Fatalln("read client config fail, timeout=0")
	}
	if len(config.ServerAddresses) == 0 {
		log.Fatalln("read client config fail, server addresses is empty")
	}
	fmt.Println("PASS")
}
