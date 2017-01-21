package shardis

import (
    "errors"
    "fmt"
    "strconv"
    "time"

    "github.com/garyburd/redigo/redis"
)

// Connection represents the connection to a single Redis instance
type Connection struct {
    Host string
    Pool *redis.Pool
}

// Create Connection instance
func NewConnection(host string, password string) (*Connection) {
    pool := makeRedisPool(host, password)
    return &Connection{
                Host: host,
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

// Redis SET on single instance
func (conn *Connection) Set(key string, value interface{}) (error) {
    cn := conn.Pool.Get()
    if cn == nil {
        return errors.New("error getting pool connection")
    }

    _, err := cn.Do("SET", key, value)
    if err != nil {
        return fmt.Errorf("error SET (%s) on host: %s", err, conn.Host)
    }
    return nil
}

// Redis GET on single instance
func (conn *Connection) Get(key string) (interface{}, error) {
    cn := conn.Pool.Get()
    if cn == nil {
        return nil, errors.New("error getting pool connection")
    }

    value, err := cn.Do("GET", key)
    if value == nil || err != nil {
        return nil, fmt.Errorf("error GET (%s) on host: %s", err, conn.Host)
    }

    value = convertType(value, err)
    return value, nil
}

// Redis RPUSH on single instance
func (conn *Connection) Rpush(key string, value interface{}) (error) {
    cn := conn.Pool.Get()
    if cn == nil {
        return errors.New("error getting pool connection")
    }

    _, err := cn.Do("RPUSH", key, value)
    if err != nil {
        return fmt.Errorf("error RPUSH (%s) on host: %s", err, conn.Host)
    }
    return nil
}

// Redis LPOP on single instance
func (conn *Connection) Lpop(key string)  (interface{}, error) {
    cn := conn.Pool.Get()
    if cn == nil {
        return nil, errors.New("error getting pool connection")
    }

    value, err := cn.Do("LPOP", key)
    if value == nil || err != nil {
        return nil, fmt.Errorf("error LPOP (%s) on host: %s", err, conn.Host)
    }

    value = convertType(value, err)
    return value, nil
}

// Convert replied data([]byte) to correct type(int or string)
func convertType(value interface{}, err error) interface{} {
  stringValue, err := redis.String(value.([]byte), err)
  if intValue, err := strconv.Atoi(stringValue); err == nil {
      return intValue
  }
  return stringValue
}
