package main

import (
	_ "embed"
	"github.com/ftkv/v1/kv"
)

//go:embed kv2-1.yaml
var kv21Config []byte

func main() {
	kv.StartServer(kv21Config)
	select {}
}
