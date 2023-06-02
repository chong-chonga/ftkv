package main

import (
	_ "embed"
	"github.com/ftkv/v1/kv"
)

//go:embed kv2-3.yaml
var kv23Config []byte

func main() {
	kv.StartServer(kv23Config)
	select {}
}
