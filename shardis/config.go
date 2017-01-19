package shardis

import (
    "encoding/json"
    _ "errors"
    _ "strings"
    "io/ioutil"
    "log"
)

type Config struct {
    Servers []map[string]string  `json:"servers"`
    HashMethod string `json:"hash"`
    Replicas int `json:"replicas"`
}

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
