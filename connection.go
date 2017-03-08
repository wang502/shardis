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
	Host         string
	Pool         *redis.Pool
	BlockTimeout int
}

// NewConnection creats a new Connection instance
func NewConnection(host string, password string, blockTimeout int) *Connection {
	pool := makeRedisPool(host, password)
	return &Connection{
		Host:         host,
		Pool:         pool,
		BlockTimeout: blockTimeout,
	}
}

// makeRedisPool creates new redis.Pool instance
// given Redis server address and password
func makeRedisPool(host string, password string) *redis.Pool {
	pool := &redis.Pool{
		MaxIdle:     5,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
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

// Set for calling Redis SET command on a single instance
func (conn *Connection) Set(key string, value interface{}) error {
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

// Get for calling Redis SET command on a single instance
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

// Lpush for calling Redis SET command on a single instance
func (conn *Connection) Lpush(key string, value interface{}) error {
	cn := conn.Pool.Get()
	if cn == nil {
		return errors.New("error getting pool connection")
	}

	_, err := cn.Do("LPUSH", key, value)
	if err != nil {
		return fmt.Errorf("error LPUSH (%s) on host: %s", err, conn.Host)
	}
	return nil
}

// Rpush for calling Redis SET command on a single instance
func (conn *Connection) Rpush(key string, value interface{}) error {
	cn := conn.Pool.Get()
	if cn == nil {
		return errors.New("error getting pool connection")
	}

	/*
	   args := make([]interface{}, len(values) + 1)
	   args[0] = key
	   for i:=1; i < len(values)+1; i++ {
	       args[i] = values[i-1]
	   }
	*/

	_, err := cn.Do("RPUSH", key, value)
	if err != nil {
		return fmt.Errorf("error RPUSH (%s) on host: %s", err, conn.Host)
	}
	return nil
}

// Lpop for calling Redis SET command on a single instance
func (conn *Connection) Lpop(key string) (interface{}, error) {
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

// Rpop for calling Redis SET command on a single instance
func (conn *Connection) Rpop(key string) (interface{}, error) {
	cn := conn.Pool.Get()
	if cn == nil {
		return nil, errors.New("error getting pool connection")
	}

	value, err := cn.Do("RPOP", key)
	if value == nil || err != nil {
		return nil, fmt.Errorf("error RPOP (%s) on host: %s", err, conn.Host)
	}

	value = convertType(value, err)
	return value, nil
}

// Blpop for calling Redis SET command on a single instance
func (conn *Connection) Blpop(keys ...string) (interface{}, error) {
	cn := conn.Pool.Get()
	if cn == nil {
		return nil, errors.New("error getting pool connection")
	}

	queueSlice := make([]interface{}, len(keys))
	for i := 0; i < len(keys); i++ {
		queueSlice[i] = keys[i]
	}
	args := append(queueSlice, conn.BlockTimeout)
	value, err := cn.Do("BLPOP", args...)

	if value == nil || err != nil {
		return nil, err
	}
	return convertType(value, err), nil
}

// Convert replied data([]byte) to correct type(int or string)
func convertType(value interface{}, err error) interface{} {
	stringValue, err := redis.String(value.([]byte), err)
	if intValue, err := strconv.Atoi(stringValue); err == nil {
		return intValue
	}
	return stringValue
}
