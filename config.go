package shardis

import (
    "encoding/json"
    _ "errors"
    _ "strings"
    "io/ioutil"
    "log"
)

// Config represents the configuration of Shardis
type Config struct {
    Servers []map[string]interface{}  `json:"servers"`
    HashMethod string `json:"hash"`
    Replicas int `json:"replicas"`
}

// Read config.json and create corresponding Config instance
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
