package conf

import (
	_ "embed"
	"encoding/json"
	"log"
	"testing"
)

//go:embed kvservice.yaml
var data []byte

func TestReadServiceYML(t *testing.T) {
	serviceConf, err := ReadConf(data)
	if err != nil {
		log.Fatalln(err)
	}
	bytes, err := json.Marshal(serviceConf)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(string(bytes))
}
