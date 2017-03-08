package shardis

import (
	"errors"
	"regexp"
)

var (
	// RegexFormat is a regex string used to extract substring enclosed by "{" and "}"
	RegexFormat = "{.*}"
)

// Shardis represents a sharis client
type Shardis struct {
	Nodes       []string
	Connections map[string]*Connection
	Ring        *HashRing
}

// New Create a Shardis instance
func New(config *Config) (*Shardis, error) {

	numServers := len(config.Servers)
	nodes := make([]string, numServers)
	connections := make(map[string]*Connection)
	for i, server := range config.Servers {
		nodes[i] = server["name"].(string)

		conn := NewConnection(server["host"].(string), "", int(server["block_timeout"].(float64)))
		connections[server["name"].(string)] = conn
	}

	ring, err := NewHashRing(nodes, config.HashMethod, config.Replicas)
	if err != nil {
		return nil, err
	}

	return &Shardis{
		Nodes:       nodes,
		Connections: connections,
		Ring:        ring,
	}, nil
}

// GetServer gets connection of the server that given key is mapped to
func (shard *Shardis) GetServer(key string) *Connection {
	serverName := shard.GetServerName(key)
	return shard.Connections[serverName]
}

// GetServerName gets name of the server that given key is hashed to
func (shard *Shardis) GetServerName(key string) string {
	re := regexp.MustCompile(RegexFormat)
	tag := re.FindString(key)
	if len(tag) == 0 {
		// for key without tag enclosed by {}, ex, "foobar"
		// directly hash the key
		tag = key
	} else {
		// ignore "{", "}"
		tag = tag[1 : len(tag)-1]
	}
	name := shard.Ring.GetNode(tag)
	return name
}

/*  Redis methods for Shardis */

// Set is for Redis SET command
func (shard *Shardis) Set(key string, value interface{}) error {
	conn := shard.GetServer(key)
	if conn == nil {
		return errors.New("no server mapped to given key")
	}
	err := conn.Set(key, value)
	return err
}

// Get is for Redis GET
func (shard *Shardis) Get(key string) (interface{}, error) {
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

// Lpush is for Redis LPUSH
func (shard *Shardis) Lpush(key string, value interface{}) error {
	conn := shard.GetServer(key)
	if conn == nil {
		return errors.New("no server mapped to given key")
	}

	err := conn.Lpush(key, value)
	return err
}

// Rpush is for Redis RPUSH
func (shard *Shardis) Rpush(key string, value interface{}) error {
	conn := shard.GetServer(key)
	if conn == nil {
		return errors.New("no server mapped to given key")
	}

	err := conn.Rpush(key, value)
	return err
}

// Lpop is for Redis LPOP
func (shard *Shardis) Lpop(key string) (interface{}, error) {
	conn := shard.GetServer(key)
	if conn == nil {
		return nil, errors.New("no server mapped to given key")
	}

	value, err := conn.Lpop(key)
	if err != nil || value == nil {
		return nil, err
	}
	return value, nil
}

// Rpop is for Redis LPOP
func (shard *Shardis) Rpop(key string) (interface{}, error) {
	conn := shard.GetServer(key)
	if conn == nil {
		return nil, errors.New("no server mapped to given key")
	}

	value, err := conn.Rpop(key)
	if err != nil || value == nil {
		return nil, err
	}
	return value, nil
}

// Blpop is for Redis BLPOP
func (shard *Shardis) Blpop(key string) (interface{}, error) {
	conn := shard.GetServer(key)
	if conn == nil {
		return nil, errors.New("no server mapped to given key")
	}

	value, err := conn.Blpop(key)
	if err != nil || value == nil {
		return nil, err
	}
	return value, nil
}
