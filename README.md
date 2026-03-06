## Redis Clone in Go

This repository contains my implementation for the
["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis),
written in Go.

## Work I've Done

- Implemented a TCP server with concurrent connection handling and RESP parsing.
- Implemented core string commands: `PING`, `ECHO`, `SET`, `GET`, `INCR`, `TYPE`, `KEYS`.
- Added key expiry handling (`EX` / `PX`) and in-memory expiration checks.
- Implemented list commands: `RPUSH`, `LPUSH`, `LRANGE`, `LLEN`, `LPOP`, `BLPOP`.
- Implemented stream commands: `XADD`, `XRANGE`, `XREAD` (including blocking behavior).
- Implemented transactions: `MULTI`, `EXEC`, `DISCARD`.
- Implemented pub/sub basics: `SUBSCRIBE`, `PUBLISH`, `UNSUBSCRIBE`.
- Implemented replication workflow:
  - `--replicaof` startup mode and handshake (`PING`, `REPLCONF`, `PSYNC`)
  - write command propagation from master to replicas
  - replica ACK tracking and `WAIT`
- Implemented config/introspection commands: `INFO`, `CONFIG GET`.
- Implemented RDB loading support from `--dir` and `--dbfilename` on startup.
- Implemented sorted set and geo command subsets:
  - Sorted set: `ZADD`, `ZRANK`, `ZRANGE`, `ZCARD`, `ZSCORE`, `ZREM`
  - Geo: `GEOADD`, `GEOPOS`, `GEODIST`, `GEOSEARCH`
- Implemented auth/ACL subset: `AUTH`, `ACL WHOAMI`, `ACL GETUSER`, `ACL SETUSER`.

## Run Locally

```sh
./your_program.sh
```

Default port is `6379`. You can also run with:

```sh
./your_program.sh --port 6380 --dir /path/to/rdb --dbfilename dump.rdb
./your_program.sh --port 6381 --replicaof "127.0.0.1 6379"
```

## Notes

This project aims to cover challenge stages incrementally and focuses on
correct protocol behavior for the implemented command subset.
