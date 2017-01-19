package main

import (
    "flag"
    "log"
    "github.com/wang502/shardis/shardis"
)

func main(){
    confPath := flag.String("c", "config.json", "path to configuration file")
    config, err := shardis.InitConfig(*confPath)
    if err != nil {
        log.Printf(err.Error())
    }
    log.Println(*config)

    shard, _ := shardis.New(config)
    log.Println(shard)

    hashedKey := shardis.ShaOne("redis shard")
    log.Println(hashedKey)

    hashRing, _ := shardis.NewHashRing([]string{"name1", "name2"}, "md5", 10)
    log.Println(hashRing.Ring)
}
