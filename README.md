# Shardis

A simple Redis sharding client. Given a key, if the key contains the {} characters, instead of hashing the whole string to obtain the instance ID, hash the string inside {} to map it to the correct instance where the value is stored. For example, key "foo" will be hashed as SHA1("foo"), the key "bar{zap}" will be hashed just as SHA1("zap"), inspired by http://oldblog.antirez.com/post/redis-presharding.html

## Installation
```
$ go get github.com/wang502/shardis
```

## Usage
### Start local Redis server
```
$ git clone git@github.com:antirez/redis.git
$ cd redis
$ ./src/redis-server
```

### Configuration
Add a config.json in your project folder
```json
{
"servers": [
      {"name": "name1",
       "host": "127.0.0.1:6379",
       "port": "",
       "db": "",
       "block_timeout": 0
      },
      {"name": "name2",
       "host": "",
       "port": "",
       "db": "",
       "block_timeout": 0
      }],
"hash": "md5",
"replicas": 10
}
```

- ***servers***: an array of servers involved in sharding. Each server json contains server name (needs to be unique), server host, port number, Redis db name, threshold of timeout for Redis blocking commands
- ***hash***: hashing method you prefer to use for sharding. (sha1, md5 and crc32)

Configuration
```go
configPath := flag.String("c", "config.json", "path to configuration file")
flag.Parse()
config, err := shardis.InitConfig(*configPath)
```
