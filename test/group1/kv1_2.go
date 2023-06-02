package main

import (
	_ "embed"
	"github.com/ftkv/v1/kv"
)

//go:embed kv1-2.yaml
var kv12Config []byte

func main() {
	kv.StartServer(kv12Config)
	select {}
}
