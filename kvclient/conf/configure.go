package conf

import (
	"errors"
	"gopkg.in/yaml.v3"
)

type KVClientConf struct {
	Addresses  []string `yaml:"addresses"`
	Password   string   `yaml:"password"`
	Timeout    int      `yaml:"timeout"`
	LogEnabled bool     `yaml:"logEnabled"`
}

func Read(configData []byte) (*KVClientConf, error) {
	if nil == configData || len(configData) == 0 {
		return nil, errors.New("configuration is empty")
	}
	conf := &KVClientConf{}
	err := yaml.Unmarshal(configData, conf)
	if err != nil {
		return nil, err
	}
	return conf, nil
}
