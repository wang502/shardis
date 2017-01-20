package shardis

import (
    "errors"
    "fmt"
    "time"

    "github.com/garyburd/redigo/redis"
)

// Connection represents a client to Redis server
type Connection struct {
    Pool *redis.Pool
}

func NewConnection(host string, password string) (*Connection) {
    pool := makeRedisPool(host, password)
    return &Connection{
                Pool: pool,
            }
}

// makeRedisPool creates new redis.Pool instance
// given Redis server address and password
func makeRedisPool(host string, password string) (*redis.Pool) {
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

/*  Redis methods for single connection*/

func (conn *Connection) Set(key string, value interface{}) (error) {
    cn := conn.Pool.Get()
    if cn == nil {
        return errors.New("error getting pool connection")
    }

    _, err := cn.Do("SET", key, value)
    if err != nil {
        return errors.New(fmt.Sprintf("error set k/v: %s", err.Error()))
    }
    return nil
}

func (conn *Connection) Get(key string) (interface{}, error) {
    cn := conn.Pool.Get()
    if cn == nil {
        return nil, errors.New("error getting pool connection")
    }

    data, err := cn.Do("GET", key)
    if data == nil || err != nil {
        return nil, errors.New("error Get on key")
    }
    return data, nil
}
