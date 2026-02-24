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

- **Streams**
  - `XADD`, `XDEL`, `XLEN`, `XTRIM`, `XRANGE`, `XREVRANGE`, `XREAD`
  - `XINFO STREAM`, `XINFO GROUPS`, `XINFO CONSUMERS`
  - `XSETID`
  - `XGROUP CREATE`, `XGROUP DESTROY`, `XGROUP SETID`, `XGROUP DELCONSUMER`
  - `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`

- **HyperLogLog**
  - `PFADD`, `PFCOUNT`, `PFMERGE`

- **Bloom filter**
  - `BF.RESERVE`, `BF.ADD`, `BF.MADD`, `BF.EXISTS`, `BF.MEXISTS`, `BF.INFO`

- **Cuckoo filter**
  - `CF.RESERVE`, `CF.ADD`, `CF.ADDNX`, `CF.INSERT`, `CF.INSERTNX`, `CF.EXISTS`, `CF.DEL`, `CF.COUNT`, `CF.INFO`

- **TDigest**
  - `TDIGEST.CREATE`, `TDIGEST.RESET`, `TDIGEST.ADD`
  - `TDIGEST.QUANTILE`, `TDIGEST.CDF`
  - `TDIGEST.RANK`, `TDIGEST.REVRANK`, `TDIGEST.BYRANK`, `TDIGEST.BYREVRANK`
  - `TDIGEST.TRIMMED_MEAN`, `TDIGEST.MIN`, `TDIGEST.MAX`, `TDIGEST.INFO`

## Quick start

```csharp
using Dredis.Abstractions.Storage;
using Dredis.Extensions.Storage.Mongo;
using MongoDB.Driver;

var client = new MongoClient("mongodb://localhost:27017");
IKeyValueStore store = new MongoKeyValueStore(client, "dredis", "kvstore");

await store.SetAsync("hello", System.Text.Encoding.UTF8.GetBytes("world"), null, SetCondition.None);
var value = await store.GetAsync("hello");
```

## Configuration

`MongoKeyValueStore` constructor:

- `mongoClient`: configured `MongoClient` instance
- `databaseName` (default: `dredis`)
- `collectionName` (default: `kvstore`)

The `collectionName` value is used as a prefix for type-specific collections (for example: `kvstore_hash`, `kvstore_list`, `kvstore_set`, `kvstore_zset`).

## Notes

- Key-type separation is implemented with dedicated Mongo collections for string/hash/list/set/sorted-set/stream/hyperloglog/bloom/cuckoo/tdigest values.
- Cross-type key operations (`DEL`, `EXISTS`, `EXPIRE`, `PTTL`, cleanup) account for all currently implemented types.
- Remaining `IKeyValueStore` areas (JSON, Vector, TimeSeries, TopK, etc.) are still pending.

## Third-party notice

- This project depends on the MongoDB .NET/C# Driver (`MongoDB.Driver`).
- MongoDB is a registered trademark of MongoDB, Inc.
- Copyright for MongoDB and the MongoDB .NET/C# Driver belongs to MongoDB, Inc.; see the package license terms for full details.
