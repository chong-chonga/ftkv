package conf

import (
	"encoding/json"
	"log"
	"testing"
)

func TestReadServiceYML(t *testing.T) {
	serviceConf, err := ReadConf()
	if err != nil {
		log.Fatalln(err)
	}
	bytes, err := json.Marshal(serviceConf)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(string(bytes))
}
