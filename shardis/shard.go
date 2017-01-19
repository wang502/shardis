package shardis

import (
    "time"
    "github.com/garyburd/redigo/redis"
)

type Shardis struct {
    Nodes []string
    Connections []*redis.Pool
    Ring *HashRing
}

func New(config *Config) (*Shardis, error) {

    num_servers := len(config.Servers)
    nodes := make([]string, num_servers)
    connections := make([]*redis.Pool, num_servers)
    for i, server := range config.Servers {
        nodes[i] = server["name"]

        conn := makeRedisPool(server["host"], "")
        connections[i] = conn

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

// makeRedisPool creates new redis.Pool instance
// given Redis server address and password
func makeRedisPool(host string, password string) *redis.Pool {
    pool := &redis.Pool{
        MaxIdle: 5,
        IdleTimeout: 240 * time.Second,
        Dial: func () (redis.Conn, error) {
              c, err := redis.Dial("tcp", host)
              if err != nil {
                  return c, nil
              }
              c.Do("AUTH", password)

              /* the is needed only if "gores" is configured in Redis's configuration file redis.conf */
              //c.Do("SELECT", "gores")
              return c, nil
            },
        TestOnBorrow: func(c redis.Conn, t time.Time) error {
            _, err := c.Do("PING")
            return err
        },
    }
    return pool
}
