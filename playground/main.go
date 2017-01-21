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

    hashRing.RemoveNode("name1")
    log.Println(hashRing.Ring)

    node := hashRing.GetNode("name2:9")
    log.Println(node)

    serverName := shard.GetServerName("abcd{name2:9}efg")
    log.Println(serverName)

    err = shard.Set("major{name1:1}", "cs")
    if err != nil {
        log.Println(err.Error())
    }

    err = shard.Set("year{name1:2}", 4)
    if err != nil {
        log.Println(err.Error())
    }
    major, err := shard.Get("major{name1:1}")
    log.Printf("major: %s\n", major)

    year, err := shard.Get("year{name1:2}")
    log.Printf("year: %d\n", year)

    /* */
    err = shard.Rpush("upvotes{name1:1}", 400)
    if err != nil {
        log.Println(err)
    }
    uv, err := shard.Lpop("upvotes{name1:1}")
    log.Printf("upvote: %d\n", uv)

    err = shard.Rpush("upvotes{name1:1}", "upvotes")
    if err != nil {
        log.Println(err)
    }
    uv1, err := shard.Lpop("upvotes{name1:1}")
    log.Printf("upvote: %s\n", uv1)
}
