package conf

import (
	_ "embed"
	"gopkg.in/yaml.v3"
)

type KVServiceConf struct {
	Me       int
	KVServer KVServerConf `yaml:"kvserver"`
	Raft     RaftConf     `yaml:"raft"`
}

type KVServerConf struct {
	// struct fields are only marshalled/unmarshalled  if they are exported
	// using the field name lower-cased as the default key
	Password           string
	Port               int    `yaml:"port"`
	MaxRaftState       int    `yaml:"maxRaftState"`
	SessionTimeout     int    `yaml:"sessionTimeout"`
	SessionIdSeparator string `yaml:"sessionIdSeparator"`
	// the content preceding the first comma is used as the key,
	// and the following comma-separated options are used to tweak the marshalling process
	// In addition, if the key is "-", the field is ignored.
	// Marshal using a flow style (useful for structs, sequences and maps).
	// 不加flow并不影响结果的正确性
}

type RaftConf struct {
	Port            int      `yaml:"port"`
	ServerAddresses []string `yaml:"server-addresses,flow"`
	RandomInterval  int64    `yaml:"randomInterval"`
	ElectionTimeout int64    `yaml:"electionTimeout"`
	Log             struct {
		RequestVoteEnabled     bool `yaml:"requestVoteEnabled"`
		AppendEntryEnabled     bool `yaml:"appendEntryEnabled"`
		InstallSnapshotEnabled bool `yaml:"installSnapshotEnabled"`
		PersistEnabled         bool `yaml:"persistEnabled"`
	}
}

//go:embed service.yml
var data []byte

func ReadConf() (*KVServiceConf, error) {
	conf := &KVServiceConf{}
	err := yaml.Unmarshal(data, conf)
	// If one or more values cannot be decoded due to a type mismatches,
	// decoding continues partially until the end of the YAML content,
	// and a *yaml.TypeError is returned with details for all missed values.
	return conf, err
}
