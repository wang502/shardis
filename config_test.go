package shardis

import (
    "testing"
)

func TestConfig(t *testing.T){
    confPath := "./config.json"
    _, err := InitConfig(confPath)
    if err != nil {
        t.Errorf("ERROR init config")
    }
}
