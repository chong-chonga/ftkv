package main

import (
	_ "embed"
	"github.com/ftkv/v1/kv"
)

//go:embed kv2-2.yaml
var kv22Config []byte

func main() {
	kv.StartServer(kv22Config)
	select {}
}
