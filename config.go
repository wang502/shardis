package shardis

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

// Config represents the configuration of Shardis
type Config struct {
	Servers    []map[string]interface{} `json:"servers"`
	HashMethod string                   `json:"hash"`
	Replicas   int                      `json:"replicas"`
}

// InitConfig reads config.json and create corresponding Config object
func InitConfig(confPath string) (*Config, error) {
	bytes, err := ioutil.ReadFile(confPath)
	log.Println(string(bytes))
	if err != nil {
		return nil, err
	}

	config := Config{}
	err = json.Unmarshal(bytes, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}
