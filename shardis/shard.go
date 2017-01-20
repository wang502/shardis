package shardis

import (
    "regexp"
)

var (
    RegexFormat = "{.*}"
)

type Shardis struct {
    Nodes []string
    Connections map[string]*Connection
    Ring *HashRing
}

func New(config *Config) (*Shardis, error) {

    num_servers := len(config.Servers)
    nodes := make([]string, num_servers)
    connections := make(map[string]*Connection)
    for i, server := range config.Servers {
        nodes[i] = server["name"]

        conn := NewConnection(server["host"], "")
        connections[server["name"]] = conn

    }

    ring, err := NewHashRing(nodes, config.HashMethod, config.Replicas)
    if err != nil {
        return nil, err
    }

    return &Shardis{
              Nodes: nodes,
              Connections: connections,
              Ring: ring,
            }, nil
}

func (shard *Shardis) GetServer(key string) (*Connection) {
    serverName := shard.GetServerName(key)
    return shard.Connections[serverName]
}

func (shard *Shardis) GetServerName(key string) (string) {
    re := regexp.MustCompile(RegexFormat)
    tag := re.FindString(key)
    if len(tag) == 0 {
        // for key without tag enclosed by {}, ex, "foobar"
        // directly hash the key
        tag = key
    }
    name := shard.Ring.GetNode(tag[1:len(tag)-1])
    return name
}

/*  Redis methods for Shardis */
func (shard *Shardis) Set(key string, value interface{}) (error) {
    conn := shard.GetServer(key)
    err := conn.Set(key, value)
    return err
}

func (shard *Shardis) Get(key string) (interface{}, error){
    conn := shard.GetServer(key)
    value, err := conn.Get(key)
    if err != nil {
        return nil, err
    }
    return value, nil
}
