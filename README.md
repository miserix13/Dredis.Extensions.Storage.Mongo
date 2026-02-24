# Dredis.Extensions.Storage.Mongo

A concrete implementation of Dredis.Abstractions.Storage for MongoDB.

## Current implementation status

This package currently implements the following `IKeyValueStore` areas:

- **String basics**
  - `SET` (`SetAsync`) with `NX`/`XX`/default semantics
  - `GET` (`GetAsync`)
  - `DEL` (`DeleteAsync`)
  - `EXISTS` (`ExistsAsync` for single and multiple keys)
  - `MGET` (`GetManyAsync`)
  - `MSET` (`SetManyAsync`)
  - `INCRBY` (`IncrByAsync`)

- **Expiration / TTL**
  - `EXPIRE` / `PEXPIRE`
  - `TTL` / `PTTL`
  - `CleanUpExpiredKeysAsync`
  - Backed by MongoDB TTL indexes and active-key filtering for immediate logical expiration behavior

- **Hashes**
  - `HSET`, `HGET`, `HDEL`, `HGETALL`

- **Lists**
  - `LPUSH`/`RPUSH` (`ListPushAsync`)
  - `LPOP`/`RPOP` (`ListPopAsync`)
  - `LRANGE` (`ListRangeAsync`)
  - `LLEN` (`ListLengthAsync`)
  - `LINDEX` (`ListIndexAsync`)
  - `LSET` (`ListSetAsync`)
  - `LTRIM` (`ListTrimAsync`)

- **Sets**
  - `SADD`, `SREM`, `SMEMBERS`, `SCARD`

- **Sorted sets**
  - `ZADD`, `ZREM`, `ZRANGE`, `ZCARD`, `ZSCORE`
  - `ZRANGEBYSCORE`, `ZINCRBY`, `ZCOUNT`
  - `ZRANK`, `ZREVRANK`, `ZREMRANGEBYSCORE`

## Notes

- Key-type separation is implemented with dedicated Mongo collections for string/hash/list/set/sorted-set values.
- Cross-type key operations (`DEL`, `EXISTS`, `EXPIRE`, `PTTL`, cleanup) account for all currently implemented types.
- Remaining `IKeyValueStore` areas (Streams, JSON, Probabilistic structures, Vector, TimeSeries, TopK, TDigest, etc.) are still pending.
