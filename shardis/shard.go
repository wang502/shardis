package shardis

import (
    "errors"
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

// Entry point
// Create a Shardis instance
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

// Get connection of the server that given key is hashed to
func (shard *Shardis) GetServer(key string) (*Connection) {
    serverName := shard.GetServerName(key)
    return shard.Connections[serverName]
}

// Get name of the server that given key is hashed to
func (shard *Shardis) GetServerName(key string) (string) {
    re := regexp.MustCompile(RegexFormat)
    tag := re.FindString(key)
    if len(tag) == 0 {
        // for key without tag enclosed by {}, ex, "foobar"
        // directly hash the key
        tag = key
    } else {
        // ignore "{", "}"
        tag = tag[1:len(tag)-1]
    }
    name := shard.Ring.GetNode(tag)
    return name
}

/*  Redis methods for Shardis */

// Redis SET
func (shard *Shardis) Set(key string, value interface{}) (error) {
    conn := shard.GetServer(key)
    if conn == nil {
        return errors.New("no server mapped to given key")
    }
    err := conn.Set(key, value)
    return err
}

// Redis GET
func (shard *Shardis) Get(key string) (interface{}, error){
    conn := shard.GetServer(key)
    if conn == nil {
        return nil, errors.New("no server mapped to given key")
    }

    value, err := conn.Get(key)
    if err != nil {
        return nil, err
    }
    return value, nil
}

// Redis RPUSH
func (shard *Shardis) Rpush(key string, value interface{}) (error) {
    conn := shard.GetServer(key)
    if conn == nil {
        return errors.New("no server mapped to given key")
    }

    err := conn.Rpush(key, value)
    return err
}
