package main

import (
	_ "embed"
	"github.com/ftkv/v1/kv"
)

//go:embed kv1-1.yaml
var kv11Config []byte

func main() {
	kv.StartServer(kv11Config)
	select {}
}
