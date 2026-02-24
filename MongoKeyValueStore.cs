using Dredis.Abstractions.Storage;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Dredis.Extensions.Storage.Mongo
{
    public class MongoKeyValueStore : IKeyValueStore
    {

        private readonly IMongoDatabase database;
        private readonly IMongoCollection<KeyValueDocument> collection;
    private readonly IMongoCollection<HashDocument> hashCollection;
    private readonly IMongoCollection<ListDocument> listCollection;
    private readonly IMongoCollection<SetDocument> setCollection;
    private readonly IMongoCollection<SortedSetDocument> sortedSetCollection;
    private readonly IMongoCollection<StreamDocument> streamCollection;
    private readonly IMongoCollection<HyperLogLogDocument> hyperLogLogCollection;
    private readonly IMongoCollection<BloomDocument> bloomCollection;
    private readonly IMongoCollection<CuckooDocument> cuckooCollection;
    private readonly IMongoCollection<TDigestDocument> tdigestCollection;

        public MongoKeyValueStore(MongoClient mongoClient, string databaseName = "dredis", string collectionName = "kvstore") : base()
        {
            this.database = mongoClient.GetDatabase(databaseName);
            this.collection = this.database.GetCollection<KeyValueDocument>(collectionName ?? "kvstore");
            this.hashCollection = this.database.GetCollection<HashDocument>($"{collectionName ?? "kvstore"}_hash");
            this.listCollection = this.database.GetCollection<ListDocument>($"{collectionName ?? "kvstore"}_list");
            this.setCollection = this.database.GetCollection<SetDocument>($"{collectionName ?? "kvstore"}_set");
            this.sortedSetCollection = this.database.GetCollection<SortedSetDocument>($"{collectionName ?? "kvstore"}_zset");
            this.streamCollection = this.database.GetCollection<StreamDocument>($"{collectionName ?? "kvstore"}_stream");
            this.hyperLogLogCollection = this.database.GetCollection<HyperLogLogDocument>($"{collectionName ?? "kvstore"}_hll");
            this.bloomCollection = this.database.GetCollection<BloomDocument>($"{collectionName ?? "kvstore"}_bloom");
            this.cuckooCollection = this.database.GetCollection<CuckooDocument>($"{collectionName ?? "kvstore"}_cuckoo");
            this.tdigestCollection = this.database.GetCollection<TDigestDocument>($"{collectionName ?? "kvstore"}_tdigest");

            var keyIndexKeys = Builders<KeyValueDocument>.IndexKeys.Ascending(x => x.Key);
            var keyIndexModel = new CreateIndexModel<KeyValueDocument>(keyIndexKeys, new CreateIndexOptions { Unique = true });
            this.collection.Indexes.CreateOne(keyIndexModel);

            var hashKeyIndexKeys = Builders<HashDocument>.IndexKeys.Ascending(x => x.Key);
            var hashKeyIndexModel = new CreateIndexModel<HashDocument>(hashKeyIndexKeys, new CreateIndexOptions { Unique = true });
            this.hashCollection.Indexes.CreateOne(hashKeyIndexModel);

            var listKeyIndexKeys = Builders<ListDocument>.IndexKeys.Ascending(x => x.Key);
            var listKeyIndexModel = new CreateIndexModel<ListDocument>(listKeyIndexKeys, new CreateIndexOptions { Unique = true });
            this.listCollection.Indexes.CreateOne(listKeyIndexModel);

            var setKeyIndexKeys = Builders<SetDocument>.IndexKeys.Ascending(x => x.Key);
            var setKeyIndexModel = new CreateIndexModel<SetDocument>(setKeyIndexKeys, new CreateIndexOptions { Unique = true });
            this.setCollection.Indexes.CreateOne(setKeyIndexModel);

            var sortedSetKeyIndexKeys = Builders<SortedSetDocument>.IndexKeys.Ascending(x => x.Key);
            var sortedSetKeyIndexModel = new CreateIndexModel<SortedSetDocument>(sortedSetKeyIndexKeys, new CreateIndexOptions { Unique = true });
            this.sortedSetCollection.Indexes.CreateOne(sortedSetKeyIndexModel);

            var streamKeyIndexKeys = Builders<StreamDocument>.IndexKeys.Ascending(x => x.Key);
            var streamKeyIndexModel = new CreateIndexModel<StreamDocument>(streamKeyIndexKeys, new CreateIndexOptions { Unique = true });
            this.streamCollection.Indexes.CreateOne(streamKeyIndexModel);

            var hllKeyIndexKeys = Builders<HyperLogLogDocument>.IndexKeys.Ascending(x => x.Key);
            var hllKeyIndexModel = new CreateIndexModel<HyperLogLogDocument>(hllKeyIndexKeys, new CreateIndexOptions { Unique = true });
            this.hyperLogLogCollection.Indexes.CreateOne(hllKeyIndexModel);

            var bloomKeyIndexKeys = Builders<BloomDocument>.IndexKeys.Ascending(x => x.Key);
            var bloomKeyIndexModel = new CreateIndexModel<BloomDocument>(bloomKeyIndexKeys, new CreateIndexOptions { Unique = true });
            this.bloomCollection.Indexes.CreateOne(bloomKeyIndexModel);

            var cuckooKeyIndexKeys = Builders<CuckooDocument>.IndexKeys.Ascending(x => x.Key);
            var cuckooKeyIndexModel = new CreateIndexModel<CuckooDocument>(cuckooKeyIndexKeys, new CreateIndexOptions { Unique = true });
            this.cuckooCollection.Indexes.CreateOne(cuckooKeyIndexModel);

            var tdigestKeyIndexKeys = Builders<TDigestDocument>.IndexKeys.Ascending(x => x.Key);
            var tdigestKeyIndexModel = new CreateIndexModel<TDigestDocument>(tdigestKeyIndexKeys, new CreateIndexOptions { Unique = true });
            this.tdigestCollection.Indexes.CreateOne(tdigestKeyIndexModel);

            // Ensure TTL index on ExpireAt
            var indexKeys = Builders<KeyValueDocument>.IndexKeys.Ascending(x => x.ExpireAt);
            var indexModel = new CreateIndexModel<KeyValueDocument>(indexKeys, new CreateIndexOptions { ExpireAfter = TimeSpan.Zero });
            this.collection.Indexes.CreateOne(indexModel);

            var hashTtlIndexKeys = Builders<HashDocument>.IndexKeys.Ascending(x => x.ExpireAt);
            var hashTtlIndexModel = new CreateIndexModel<HashDocument>(hashTtlIndexKeys, new CreateIndexOptions { ExpireAfter = TimeSpan.Zero });
            this.hashCollection.Indexes.CreateOne(hashTtlIndexModel);

            var listTtlIndexKeys = Builders<ListDocument>.IndexKeys.Ascending(x => x.ExpireAt);
            var listTtlIndexModel = new CreateIndexModel<ListDocument>(listTtlIndexKeys, new CreateIndexOptions { ExpireAfter = TimeSpan.Zero });
            this.listCollection.Indexes.CreateOne(listTtlIndexModel);

            var setTtlIndexKeys = Builders<SetDocument>.IndexKeys.Ascending(x => x.ExpireAt);
            var setTtlIndexModel = new CreateIndexModel<SetDocument>(setTtlIndexKeys, new CreateIndexOptions { ExpireAfter = TimeSpan.Zero });
            this.setCollection.Indexes.CreateOne(setTtlIndexModel);

            var sortedSetTtlIndexKeys = Builders<SortedSetDocument>.IndexKeys.Ascending(x => x.ExpireAt);
            var sortedSetTtlIndexModel = new CreateIndexModel<SortedSetDocument>(sortedSetTtlIndexKeys, new CreateIndexOptions { ExpireAfter = TimeSpan.Zero });
            this.sortedSetCollection.Indexes.CreateOne(sortedSetTtlIndexModel);

            var streamTtlIndexKeys = Builders<StreamDocument>.IndexKeys.Ascending(x => x.ExpireAt);
            var streamTtlIndexModel = new CreateIndexModel<StreamDocument>(streamTtlIndexKeys, new CreateIndexOptions { ExpireAfter = TimeSpan.Zero });
            this.streamCollection.Indexes.CreateOne(streamTtlIndexModel);

            var hllTtlIndexKeys = Builders<HyperLogLogDocument>.IndexKeys.Ascending(x => x.ExpireAt);
            var hllTtlIndexModel = new CreateIndexModel<HyperLogLogDocument>(hllTtlIndexKeys, new CreateIndexOptions { ExpireAfter = TimeSpan.Zero });
            this.hyperLogLogCollection.Indexes.CreateOne(hllTtlIndexModel);

            var bloomTtlIndexKeys = Builders<BloomDocument>.IndexKeys.Ascending(x => x.ExpireAt);
            var bloomTtlIndexModel = new CreateIndexModel<BloomDocument>(bloomTtlIndexKeys, new CreateIndexOptions { ExpireAfter = TimeSpan.Zero });
            this.bloomCollection.Indexes.CreateOne(bloomTtlIndexModel);

            var cuckooTtlIndexKeys = Builders<CuckooDocument>.IndexKeys.Ascending(x => x.ExpireAt);
            var cuckooTtlIndexModel = new CreateIndexModel<CuckooDocument>(cuckooTtlIndexKeys, new CreateIndexOptions { ExpireAfter = TimeSpan.Zero });
            this.cuckooCollection.Indexes.CreateOne(cuckooTtlIndexModel);

            var tdigestTtlIndexKeys = Builders<TDigestDocument>.IndexKeys.Ascending(x => x.ExpireAt);
            var tdigestTtlIndexModel = new CreateIndexModel<TDigestDocument>(tdigestTtlIndexKeys, new CreateIndexOptions { ExpireAfter = TimeSpan.Zero });
            this.tdigestCollection.Indexes.CreateOne(tdigestTtlIndexModel);
        }

        private class KeyValueDocument
        {
            public ObjectId Id { get; set; }
            public string Key { get; set; } = null!;
            public byte[] Value { get; set; } = null!;
            public DateTime? ExpireAt { get; set; }
        }

        private class HashFieldDocument
        {
            public string Field { get; set; } = null!;
            public byte[] Value { get; set; } = null!;
        }

        private class HashDocument
        {
            public ObjectId Id { get; set; }
            public string Key { get; set; } = null!;
            public List<HashFieldDocument> Fields { get; set; } = new();
            public DateTime? ExpireAt { get; set; }
        }

        private class ListDocument
        {
            public ObjectId Id { get; set; }
            public string Key { get; set; } = null!;
            public List<byte[]> Values { get; set; } = new();
            public DateTime? ExpireAt { get; set; }
        }

        private class SetMemberDocument
        {
            public string MemberKey { get; set; } = null!;
            public byte[] Member { get; set; } = null!;
        }

        private class SetDocument
        {
            public ObjectId Id { get; set; }
            public string Key { get; set; } = null!;
            public List<SetMemberDocument> Members { get; set; } = new();
            public DateTime? ExpireAt { get; set; }
        }

        private class SortedSetMemberDocument
        {
            public string MemberKey { get; set; } = null!;
            public byte[] Member { get; set; } = null!;
            public double Score { get; set; }
        }

        private class SortedSetDocument
        {
            public ObjectId Id { get; set; }
            public string Key { get; set; } = null!;
            public List<SortedSetMemberDocument> Members { get; set; } = new();
            public DateTime? ExpireAt { get; set; }
        }

        private class StreamFieldDocument
        {
            public string Field { get; set; } = null!;
            public byte[] Value { get; set; } = null!;
        }

        private class StreamEntryDocument
        {
            public string Id { get; set; } = null!;
            public long Milliseconds { get; set; }
            public long Sequence { get; set; }
            public List<StreamFieldDocument> Fields { get; set; } = new();
        }

        private class StreamDocument
        {
            public ObjectId Id { get; set; }
            public string Key { get; set; } = null!;
            public List<StreamEntryDocument> Entries { get; set; } = new();
            public string? LastId { get; set; }
            public List<StreamGroupDocument> Groups { get; set; } = new();
            public DateTime? ExpireAt { get; set; }
        }

        private class StreamGroupDocument
        {
            public string Name { get; set; } = null!;
            public string LastDeliveredId { get; set; } = "0-0";
            public List<StreamConsumerDocument> Consumers { get; set; } = new();
            public List<StreamPendingDocument> Pending { get; set; } = new();
        }

        private class StreamConsumerDocument
        {
            public string Name { get; set; } = null!;
            public long LastSeenUnixMs { get; set; }
        }

        private class StreamPendingDocument
        {
            public string Id { get; set; } = null!;
            public string Consumer { get; set; } = null!;
            public long LastDeliveryUnixMs { get; set; }
            public long DeliveryCount { get; set; }
        }

        private class HyperLogLogMemberDocument
        {
            public string MemberKey { get; set; } = null!;
            public byte[] Member { get; set; } = null!;
        }

        private class HyperLogLogDocument
        {
            public ObjectId Id { get; set; }
            public string Key { get; set; } = null!;
            public List<HyperLogLogMemberDocument> Members { get; set; } = new();
            public DateTime? ExpireAt { get; set; }
        }

        private class BloomDocument
        {
            public ObjectId Id { get; set; }
            public string Key { get; set; } = null!;
            public double ErrorRate { get; set; }
            public long Capacity { get; set; }
            public int HashFunctions { get; set; }
            public long BitSize { get; set; }
            public byte[] Bits { get; set; } = Array.Empty<byte>();
            public long ItemsInserted { get; set; }
            public DateTime? ExpireAt { get; set; }
        }

        private class CuckooItemDocument
        {
            public string ItemKey { get; set; } = null!;
            public byte[] Item { get; set; } = null!;
            public long Count { get; set; }
        }

        private class CuckooDocument
        {
            public ObjectId Id { get; set; }
            public string Key { get; set; } = null!;
            public long Capacity { get; set; }
            public List<CuckooItemDocument> Items { get; set; } = new();
            public DateTime? ExpireAt { get; set; }
        }

        private class TDigestDocument
        {
            public ObjectId Id { get; set; }
            public string Key { get; set; } = null!;
            public int Compression { get; set; }
            public List<double> Values { get; set; } = new();
            public DateTime? ExpireAt { get; set; }
        }

        private FilterDefinition<KeyValueDocument> KeyFilter(string key) => Builders<KeyValueDocument>.Filter.Eq(x => x.Key, key);

        private static FilterDefinition<KeyValueDocument> ActiveFilter(DateTime nowUtc) =>
            Builders<KeyValueDocument>.Filter.Or(
                Builders<KeyValueDocument>.Filter.Eq(x => x.ExpireAt, (DateTime?)null),
                Builders<KeyValueDocument>.Filter.Gt(x => x.ExpireAt, nowUtc));

        private FilterDefinition<KeyValueDocument> ActiveKeyFilter(string key, DateTime nowUtc) =>
            Builders<KeyValueDocument>.Filter.And(KeyFilter(key), ActiveFilter(nowUtc));

        private FilterDefinition<KeyValueDocument> ActiveKeysFilter(string[] keys, DateTime nowUtc) =>
            Builders<KeyValueDocument>.Filter.And(
                Builders<KeyValueDocument>.Filter.In(x => x.Key, keys),
                ActiveFilter(nowUtc));

        private FilterDefinition<HashDocument> HashKeyFilter(string key) => Builders<HashDocument>.Filter.Eq(x => x.Key, key);

        private static FilterDefinition<HashDocument> ActiveHashFilter(DateTime nowUtc) =>
            Builders<HashDocument>.Filter.Or(
                Builders<HashDocument>.Filter.Eq(x => x.ExpireAt, (DateTime?)null),
                Builders<HashDocument>.Filter.Gt(x => x.ExpireAt, nowUtc));

        private FilterDefinition<HashDocument> ActiveHashKeyFilter(string key, DateTime nowUtc) =>
            Builders<HashDocument>.Filter.And(HashKeyFilter(key), ActiveHashFilter(nowUtc));

        private FilterDefinition<ListDocument> ListKeyFilter(string key) => Builders<ListDocument>.Filter.Eq(x => x.Key, key);

        private static FilterDefinition<ListDocument> ActiveListFilter(DateTime nowUtc) =>
            Builders<ListDocument>.Filter.Or(
                Builders<ListDocument>.Filter.Eq(x => x.ExpireAt, (DateTime?)null),
                Builders<ListDocument>.Filter.Gt(x => x.ExpireAt, nowUtc));

        private FilterDefinition<ListDocument> ActiveListKeyFilter(string key, DateTime nowUtc) =>
            Builders<ListDocument>.Filter.And(ListKeyFilter(key), ActiveListFilter(nowUtc));

        private FilterDefinition<ListDocument> ActiveListKeysFilter(string[] keys, DateTime nowUtc) =>
            Builders<ListDocument>.Filter.And(
                Builders<ListDocument>.Filter.In(x => x.Key, keys),
                ActiveListFilter(nowUtc));

        private FilterDefinition<SetDocument> SetKeyFilter(string key) => Builders<SetDocument>.Filter.Eq(x => x.Key, key);

        private static FilterDefinition<SetDocument> ActiveSetFilter(DateTime nowUtc) =>
            Builders<SetDocument>.Filter.Or(
                Builders<SetDocument>.Filter.Eq(x => x.ExpireAt, (DateTime?)null),
                Builders<SetDocument>.Filter.Gt(x => x.ExpireAt, nowUtc));

        private FilterDefinition<SetDocument> ActiveSetKeyFilter(string key, DateTime nowUtc) =>
            Builders<SetDocument>.Filter.And(SetKeyFilter(key), ActiveSetFilter(nowUtc));

        private FilterDefinition<SetDocument> ActiveSetKeysFilter(string[] keys, DateTime nowUtc) =>
            Builders<SetDocument>.Filter.And(
                Builders<SetDocument>.Filter.In(x => x.Key, keys),
                ActiveSetFilter(nowUtc));

        private FilterDefinition<SortedSetDocument> SortedSetKeyFilter(string key) => Builders<SortedSetDocument>.Filter.Eq(x => x.Key, key);

        private static FilterDefinition<SortedSetDocument> ActiveSortedSetFilter(DateTime nowUtc) =>
            Builders<SortedSetDocument>.Filter.Or(
                Builders<SortedSetDocument>.Filter.Eq(x => x.ExpireAt, (DateTime?)null),
                Builders<SortedSetDocument>.Filter.Gt(x => x.ExpireAt, nowUtc));

        private FilterDefinition<SortedSetDocument> ActiveSortedSetKeyFilter(string key, DateTime nowUtc) =>
            Builders<SortedSetDocument>.Filter.And(SortedSetKeyFilter(key), ActiveSortedSetFilter(nowUtc));

        private FilterDefinition<SortedSetDocument> ActiveSortedSetKeysFilter(string[] keys, DateTime nowUtc) =>
            Builders<SortedSetDocument>.Filter.And(
                Builders<SortedSetDocument>.Filter.In(x => x.Key, keys),
                ActiveSortedSetFilter(nowUtc));

        private FilterDefinition<StreamDocument> StreamKeyFilter(string key) => Builders<StreamDocument>.Filter.Eq(x => x.Key, key);

        private static FilterDefinition<StreamDocument> ActiveStreamFilter(DateTime nowUtc) =>
            Builders<StreamDocument>.Filter.Or(
                Builders<StreamDocument>.Filter.Eq(x => x.ExpireAt, (DateTime?)null),
                Builders<StreamDocument>.Filter.Gt(x => x.ExpireAt, nowUtc));

        private FilterDefinition<StreamDocument> ActiveStreamKeyFilter(string key, DateTime nowUtc) =>
            Builders<StreamDocument>.Filter.And(StreamKeyFilter(key), ActiveStreamFilter(nowUtc));

        private FilterDefinition<StreamDocument> ActiveStreamKeysFilter(string[] keys, DateTime nowUtc) =>
            Builders<StreamDocument>.Filter.And(
                Builders<StreamDocument>.Filter.In(x => x.Key, keys),
                ActiveStreamFilter(nowUtc));

        private FilterDefinition<HyperLogLogDocument> HyperLogLogKeyFilter(string key) => Builders<HyperLogLogDocument>.Filter.Eq(x => x.Key, key);

        private static FilterDefinition<HyperLogLogDocument> ActiveHyperLogLogFilter(DateTime nowUtc) =>
            Builders<HyperLogLogDocument>.Filter.Or(
                Builders<HyperLogLogDocument>.Filter.Eq(x => x.ExpireAt, (DateTime?)null),
                Builders<HyperLogLogDocument>.Filter.Gt(x => x.ExpireAt, nowUtc));

        private FilterDefinition<HyperLogLogDocument> ActiveHyperLogLogKeyFilter(string key, DateTime nowUtc) =>
            Builders<HyperLogLogDocument>.Filter.And(HyperLogLogKeyFilter(key), ActiveHyperLogLogFilter(nowUtc));

        private FilterDefinition<HyperLogLogDocument> ActiveHyperLogLogKeysFilter(string[] keys, DateTime nowUtc) =>
            Builders<HyperLogLogDocument>.Filter.And(
                Builders<HyperLogLogDocument>.Filter.In(x => x.Key, keys),
                ActiveHyperLogLogFilter(nowUtc));

        private FilterDefinition<BloomDocument> BloomKeyFilter(string key) => Builders<BloomDocument>.Filter.Eq(x => x.Key, key);

        private static FilterDefinition<BloomDocument> ActiveBloomFilter(DateTime nowUtc) =>
            Builders<BloomDocument>.Filter.Or(
                Builders<BloomDocument>.Filter.Eq(x => x.ExpireAt, (DateTime?)null),
                Builders<BloomDocument>.Filter.Gt(x => x.ExpireAt, nowUtc));

        private FilterDefinition<BloomDocument> ActiveBloomKeyFilter(string key, DateTime nowUtc) =>
            Builders<BloomDocument>.Filter.And(BloomKeyFilter(key), ActiveBloomFilter(nowUtc));

        private FilterDefinition<BloomDocument> ActiveBloomKeysFilter(string[] keys, DateTime nowUtc) =>
            Builders<BloomDocument>.Filter.And(
                Builders<BloomDocument>.Filter.In(x => x.Key, keys),
                ActiveBloomFilter(nowUtc));

        private FilterDefinition<CuckooDocument> CuckooKeyFilter(string key) => Builders<CuckooDocument>.Filter.Eq(x => x.Key, key);

        private static FilterDefinition<CuckooDocument> ActiveCuckooFilter(DateTime nowUtc) =>
            Builders<CuckooDocument>.Filter.Or(
                Builders<CuckooDocument>.Filter.Eq(x => x.ExpireAt, (DateTime?)null),
                Builders<CuckooDocument>.Filter.Gt(x => x.ExpireAt, nowUtc));

        private FilterDefinition<CuckooDocument> ActiveCuckooKeyFilter(string key, DateTime nowUtc) =>
            Builders<CuckooDocument>.Filter.And(CuckooKeyFilter(key), ActiveCuckooFilter(nowUtc));

        private FilterDefinition<CuckooDocument> ActiveCuckooKeysFilter(string[] keys, DateTime nowUtc) =>
            Builders<CuckooDocument>.Filter.And(
                Builders<CuckooDocument>.Filter.In(x => x.Key, keys),
                ActiveCuckooFilter(nowUtc));

        private FilterDefinition<TDigestDocument> TDigestKeyFilter(string key) => Builders<TDigestDocument>.Filter.Eq(x => x.Key, key);

        private static FilterDefinition<TDigestDocument> ActiveTDigestFilter(DateTime nowUtc) =>
            Builders<TDigestDocument>.Filter.Or(
                Builders<TDigestDocument>.Filter.Eq(x => x.ExpireAt, (DateTime?)null),
                Builders<TDigestDocument>.Filter.Gt(x => x.ExpireAt, nowUtc));

        private FilterDefinition<TDigestDocument> ActiveTDigestKeyFilter(string key, DateTime nowUtc) =>
            Builders<TDigestDocument>.Filter.And(TDigestKeyFilter(key), ActiveTDigestFilter(nowUtc));

        private FilterDefinition<TDigestDocument> ActiveTDigestKeysFilter(string[] keys, DateTime nowUtc) =>
            Builders<TDigestDocument>.Filter.And(
                Builders<TDigestDocument>.Filter.In(x => x.Key, keys),
                ActiveTDigestFilter(nowUtc));

        private FilterDefinition<HashDocument> ActiveHashKeysFilter(string[] keys, DateTime nowUtc) =>
            Builders<HashDocument>.Filter.And(
                Builders<HashDocument>.Filter.In(x => x.Key, keys),
                ActiveHashFilter(nowUtc));

        public async Task<bool> SetAsync(string key, byte[] value, TimeSpan? expiration, SetCondition condition, CancellationToken token = default)
        {
            var nowUtc = DateTime.UtcNow;
            DateTime? expireAt = expiration.HasValue ? nowUtc.Add(expiration.Value) : (DateTime?)null;
            var filter = KeyFilter(key);
            var update = Builders<KeyValueDocument>.Update
                .Set(x => x.Value, value)
                .Set(x => x.Key, key)
                .Set(x => x.ExpireAt, expireAt);

            UpdateOptions options = new() { IsUpsert = true };
            if (condition == SetCondition.Nx)
            {
                var deletedExpired = await this.collection.DeleteManyAsync(
                    Builders<KeyValueDocument>.Filter.And(
                        KeyFilter(key),
                        Builders<KeyValueDocument>.Filter.Lte(x => x.ExpireAt, nowUtc)),
                    token);

                var doc = new KeyValueDocument { Key = key, Value = value, ExpireAt = expireAt };
                try
                {
                    await this.collection.InsertOneAsync(doc, null, token);
                    return true;
                }
                catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
                {
                    return false;
                }
            }
            else if (condition == SetCondition.Xx)
            {
                var updateResult = await this.collection.UpdateOneAsync(ActiveKeyFilter(key, nowUtc), update, new UpdateOptions { IsUpsert = false }, token);
                return updateResult.MatchedCount > 0;
            }
            else
            {
                var updateResult = await collection.UpdateOneAsync(filter, update, options, token);
                return updateResult.MatchedCount > 0 || updateResult.ModifiedCount > 0 || updateResult.UpsertedId != null;
            }
        }

        public async Task<byte[]?> GetAsync(string key, CancellationToken token = default)
        {
            var filter = ActiveKeyFilter(key, DateTime.UtcNow);
            var doc = await collection.Find(filter).FirstOrDefaultAsync(token);
            if (doc == null)
                return null;
            return doc.Value;
        }

        public async Task<long> DeleteAsync(string[] keys, CancellationToken token = default)
        {
            if (keys.Length == 0)
            {
                return 0;
            }

            var filter = Builders<KeyValueDocument>.Filter.In(x => x.Key, keys);
            var result = await collection.DeleteManyAsync(filter, token);
            var hashFilter = Builders<HashDocument>.Filter.In(x => x.Key, keys);
            var hashResult = await hashCollection.DeleteManyAsync(hashFilter, token);
            var listFilter = Builders<ListDocument>.Filter.In(x => x.Key, keys);
            var listResult = await listCollection.DeleteManyAsync(listFilter, token);
            var setFilter = Builders<SetDocument>.Filter.In(x => x.Key, keys);
            var setResult = await setCollection.DeleteManyAsync(setFilter, token);
            var sortedSetFilter = Builders<SortedSetDocument>.Filter.In(x => x.Key, keys);
            var sortedSetResult = await sortedSetCollection.DeleteManyAsync(sortedSetFilter, token);
            var streamFilter = Builders<StreamDocument>.Filter.In(x => x.Key, keys);
            var streamResult = await streamCollection.DeleteManyAsync(streamFilter, token);
            var hllFilter = Builders<HyperLogLogDocument>.Filter.In(x => x.Key, keys);
            var hllResult = await hyperLogLogCollection.DeleteManyAsync(hllFilter, token);
            var bloomFilter = Builders<BloomDocument>.Filter.In(x => x.Key, keys);
            var bloomResult = await bloomCollection.DeleteManyAsync(bloomFilter, token);
            var cuckooFilter = Builders<CuckooDocument>.Filter.In(x => x.Key, keys);
            var cuckooResult = await cuckooCollection.DeleteManyAsync(cuckooFilter, token);
            var tdigestFilter = Builders<TDigestDocument>.Filter.In(x => x.Key, keys);
            var tdigestResult = await tdigestCollection.DeleteManyAsync(tdigestFilter, token);
            return result.DeletedCount + hashResult.DeletedCount + listResult.DeletedCount + setResult.DeletedCount + sortedSetResult.DeletedCount + streamResult.DeletedCount + hllResult.DeletedCount + bloomResult.DeletedCount + cuckooResult.DeletedCount + tdigestResult.DeletedCount;
        }

        public async Task<bool> ExistsAsync(string key, CancellationToken token = default)
        {
            var nowUtc = DateTime.UtcNow;
            var stringCount = await collection.CountDocumentsAsync(ActiveKeyFilter(key, nowUtc), null, token);
            if (stringCount > 0)
            {
                return true;
            }

            var hashCount = await hashCollection.CountDocumentsAsync(ActiveHashKeyFilter(key, nowUtc), null, token);
            if (hashCount > 0)
            {
                return true;
            }

            var listCount = await listCollection.CountDocumentsAsync(ActiveListKeyFilter(key, nowUtc), null, token);
            if (listCount > 0)
            {
                return true;
            }

            var setCount = await setCollection.CountDocumentsAsync(ActiveSetKeyFilter(key, nowUtc), null, token);
            if (setCount > 0)
            {
                return true;
            }

            var sortedSetCount = await sortedSetCollection.CountDocumentsAsync(ActiveSortedSetKeyFilter(key, nowUtc), null, token);
            if (sortedSetCount > 0)
            {
                return true;
            }

            var streamCount = await streamCollection.CountDocumentsAsync(ActiveStreamKeyFilter(key, nowUtc), null, token);
            if (streamCount > 0)
            {
                return true;
            }

            var hllCount = await hyperLogLogCollection.CountDocumentsAsync(ActiveHyperLogLogKeyFilter(key, nowUtc), null, token);
            if (hllCount > 0)
            {
                return true;
            }

            var bloomCount = await bloomCollection.CountDocumentsAsync(ActiveBloomKeyFilter(key, nowUtc), null, token);
            if (bloomCount > 0)
            {
                return true;
            }

            var cuckooCount = await cuckooCollection.CountDocumentsAsync(ActiveCuckooKeyFilter(key, nowUtc), null, token);
            if (cuckooCount > 0)
            {
                return true;
            }

            return await tdigestCollection.CountDocumentsAsync(ActiveTDigestKeyFilter(key, nowUtc), null, token) > 0;
        }

        public async Task<long> ExistsAsync(string[] keys, CancellationToken token = default)
        {
            if (keys.Length == 0)
            {
                return 0;
            }

            var nowUtc = DateTime.UtcNow;
            var keySet = new HashSet<string>(StringComparer.Ordinal);

            var stringKeys = await collection.Find(ActiveKeysFilter(keys, nowUtc)).Project(x => x.Key).ToListAsync(token);
            foreach (var item in stringKeys)
            {
                keySet.Add(item);
            }

            var hashKeys = await hashCollection.Find(ActiveHashKeysFilter(keys, nowUtc)).Project(x => x.Key).ToListAsync(token);
            foreach (var item in hashKeys)
            {
                keySet.Add(item);
            }

            var listKeys = await listCollection.Find(ActiveListKeysFilter(keys, nowUtc)).Project(x => x.Key).ToListAsync(token);
            foreach (var item in listKeys)
            {
                keySet.Add(item);
            }

            var setKeys = await setCollection.Find(ActiveSetKeysFilter(keys, nowUtc)).Project(x => x.Key).ToListAsync(token);
            foreach (var item in setKeys)
            {
                keySet.Add(item);
            }

            var sortedSetKeys = await sortedSetCollection.Find(ActiveSortedSetKeysFilter(keys, nowUtc)).Project(x => x.Key).ToListAsync(token);
            foreach (var item in sortedSetKeys)
            {
                keySet.Add(item);
            }

            var streamKeys = await streamCollection.Find(ActiveStreamKeysFilter(keys, nowUtc)).Project(x => x.Key).ToListAsync(token);
            foreach (var item in streamKeys)
            {
                keySet.Add(item);
            }

            var hllKeys = await hyperLogLogCollection.Find(ActiveHyperLogLogKeysFilter(keys, nowUtc)).Project(x => x.Key).ToListAsync(token);
            foreach (var item in hllKeys)
            {
                keySet.Add(item);
            }

            var bloomKeys = await bloomCollection.Find(ActiveBloomKeysFilter(keys, nowUtc)).Project(x => x.Key).ToListAsync(token);
            foreach (var item in bloomKeys)
            {
                keySet.Add(item);
            }

            var cuckooKeys = await cuckooCollection.Find(ActiveCuckooKeysFilter(keys, nowUtc)).Project(x => x.Key).ToListAsync(token);
            foreach (var item in cuckooKeys)
            {
                keySet.Add(item);
            }

            var tdigestKeys = await tdigestCollection.Find(ActiveTDigestKeysFilter(keys, nowUtc)).Project(x => x.Key).ToListAsync(token);
            foreach (var item in tdigestKeys)
            {
                keySet.Add(item);
            }

            return keySet.Count;
        }

        public async Task<bool> ExpireAsync(string key, TimeSpan expiration, CancellationToken token = default)
        {
            var nowUtc = DateTime.UtcNow;
            if (expiration <= TimeSpan.Zero)
            {
                var deleted = 0L;
                deleted += (await collection.DeleteOneAsync(ActiveKeyFilter(key, nowUtc), token)).DeletedCount;
                deleted += (await hashCollection.DeleteOneAsync(ActiveHashKeyFilter(key, nowUtc), token)).DeletedCount;
                deleted += (await listCollection.DeleteOneAsync(ActiveListKeyFilter(key, nowUtc), token)).DeletedCount;
                deleted += (await setCollection.DeleteOneAsync(ActiveSetKeyFilter(key, nowUtc), token)).DeletedCount;
                deleted += (await sortedSetCollection.DeleteOneAsync(ActiveSortedSetKeyFilter(key, nowUtc), token)).DeletedCount;
                deleted += (await streamCollection.DeleteOneAsync(ActiveStreamKeyFilter(key, nowUtc), token)).DeletedCount;
                deleted += (await hyperLogLogCollection.DeleteOneAsync(ActiveHyperLogLogKeyFilter(key, nowUtc), token)).DeletedCount;
                deleted += (await bloomCollection.DeleteOneAsync(ActiveBloomKeyFilter(key, nowUtc), token)).DeletedCount;
                deleted += (await cuckooCollection.DeleteOneAsync(ActiveCuckooKeyFilter(key, nowUtc), token)).DeletedCount;
                deleted += (await tdigestCollection.DeleteOneAsync(ActiveTDigestKeyFilter(key, nowUtc), token)).DeletedCount;
                return deleted > 0;
            }

            var expireAt = nowUtc.Add(expiration);

            var filter = ActiveKeyFilter(key, nowUtc);
            var update = Builders<KeyValueDocument>.Update.Set(x => x.ExpireAt, expireAt);
            var result = await collection.UpdateOneAsync(filter, update, null, token);
            if (result.MatchedCount > 0)
            {
                return true;
            }

            var hashFilter = ActiveHashKeyFilter(key, nowUtc);
            var hashUpdate = Builders<HashDocument>.Update.Set(x => x.ExpireAt, expireAt);
            var hashResult = await hashCollection.UpdateOneAsync(hashFilter, hashUpdate, null, token);
            if (hashResult.MatchedCount > 0)
            {
                return true;
            }

            var listFilter = ActiveListKeyFilter(key, nowUtc);
            var listUpdate = Builders<ListDocument>.Update.Set(x => x.ExpireAt, expireAt);
            var listResult = await listCollection.UpdateOneAsync(listFilter, listUpdate, null, token);
            if (listResult.MatchedCount > 0)
            {
                return true;
            }

            var setFilter = ActiveSetKeyFilter(key, nowUtc);
            var setUpdate = Builders<SetDocument>.Update.Set(x => x.ExpireAt, expireAt);
            var setResult = await setCollection.UpdateOneAsync(setFilter, setUpdate, null, token);
            if (setResult.MatchedCount > 0)
            {
                return true;
            }

            var sortedSetFilter = ActiveSortedSetKeyFilter(key, nowUtc);
            var sortedSetUpdate = Builders<SortedSetDocument>.Update.Set(x => x.ExpireAt, expireAt);
            var sortedSetResult = await sortedSetCollection.UpdateOneAsync(sortedSetFilter, sortedSetUpdate, null, token);
            if (sortedSetResult.MatchedCount > 0)
            {
                return true;
            }

            var streamFilter = ActiveStreamKeyFilter(key, nowUtc);
            var streamUpdate = Builders<StreamDocument>.Update.Set(x => x.ExpireAt, expireAt);
            var streamResult = await streamCollection.UpdateOneAsync(streamFilter, streamUpdate, null, token);
            if (streamResult.MatchedCount > 0)
            {
                return true;
            }

            var hllFilter = ActiveHyperLogLogKeyFilter(key, nowUtc);
            var hllUpdate = Builders<HyperLogLogDocument>.Update.Set(x => x.ExpireAt, expireAt);
            var hllResult = await hyperLogLogCollection.UpdateOneAsync(hllFilter, hllUpdate, null, token);
            if (hllResult.MatchedCount > 0)
            {
                return true;
            }

            var bloomFilter = ActiveBloomKeyFilter(key, nowUtc);
            var bloomUpdate = Builders<BloomDocument>.Update.Set(x => x.ExpireAt, expireAt);
            var bloomResult = await bloomCollection.UpdateOneAsync(bloomFilter, bloomUpdate, null, token);
            if (bloomResult.MatchedCount > 0)
            {
                return true;
            }

            var cuckooFilter = ActiveCuckooKeyFilter(key, nowUtc);
            var cuckooUpdate = Builders<CuckooDocument>.Update.Set(x => x.ExpireAt, expireAt);
            var cuckooResult = await cuckooCollection.UpdateOneAsync(cuckooFilter, cuckooUpdate, null, token);
            if (cuckooResult.MatchedCount > 0)
            {
                return true;
            }

            var tdigestFilter = ActiveTDigestKeyFilter(key, nowUtc);
            var tdigestUpdate = Builders<TDigestDocument>.Update.Set(x => x.ExpireAt, expireAt);
            var tdigestResult = await tdigestCollection.UpdateOneAsync(tdigestFilter, tdigestUpdate, null, token);
            return tdigestResult.MatchedCount > 0;
        }

        public Task<ProbabilisticBoolResult> BloomAddAsync(string key, byte[] element, CancellationToken token = default)
        {
            return BloomAddCoreAsync(key, element, token);
        }

        public Task<ProbabilisticBoolResult> BloomExistsAsync(string key, byte[] element, CancellationToken token = default)
        {
            return BloomExistsCoreAsync(key, element, token);
        }

        public Task<ProbabilisticInfoResult> BloomInfoAsync(string key, CancellationToken token = default)
        {
            return BloomInfoCoreAsync(key, token);
        }

        public Task<ProbabilisticArrayResult> BloomMAddAsync(string key, byte[][] elements, CancellationToken token = default)
        {
            return BloomMAddCoreAsync(key, elements, token);
        }

        public Task<ProbabilisticArrayResult> BloomMExistsAsync(string key, byte[][] elements, CancellationToken token = default)
        {
            return BloomMExistsCoreAsync(key, elements, token);
        }

        public Task<ProbabilisticResultStatus> BloomReserveAsync(string key, double errorRate, long capacity, CancellationToken token = default)
        {
            return BloomReserveCoreAsync(key, errorRate, capacity, token);
        }

        public Task<long> CleanUpExpiredKeysAsync(CancellationToken token = default)
        {
            return CleanUpExpiredKeysCoreAsync(token);
        }

        public Task<ProbabilisticBoolResult> CuckooAddAsync(string key, byte[] item, bool noCreate, CancellationToken token = default)
        {
            return CuckooAddCoreAsync(key, item, noCreate, nx: false, token);
        }

        public Task<ProbabilisticBoolResult> CuckooAddNxAsync(string key, byte[] item, bool noCreate, CancellationToken token = default)
        {
            return CuckooAddCoreAsync(key, item, noCreate, nx: true, token);
        }

        public Task<ProbabilisticCountResult> CuckooCountAsync(string key, byte[] item, CancellationToken token = default)
        {
            return CuckooCountCoreAsync(key, item, token);
        }

        public Task<ProbabilisticBoolResult> CuckooDeleteAsync(string key, byte[] item, CancellationToken token = default)
        {
            return CuckooDeleteCoreAsync(key, item, token);
        }

        public Task<ProbabilisticBoolResult> CuckooExistsAsync(string key, byte[] item, CancellationToken token = default)
        {
            return CuckooExistsCoreAsync(key, item, token);
        }

        public Task<ProbabilisticInfoResult> CuckooInfoAsync(string key, CancellationToken token = default)
        {
            return CuckooInfoCoreAsync(key, token);
        }

        public Task<ProbabilisticResultStatus> CuckooReserveAsync(string key, long capacity, CancellationToken token = default)
        {
            return CuckooReserveCoreAsync(key, capacity, token);
        }

        public Task<byte[]?[]> GetManyAsync(string[] keys, CancellationToken token = default)
        {
            return GetManyCoreAsync(keys, token);
        }

        public Task<long> HashDeleteAsync(string key, string[] fields, CancellationToken token = default)
        {
            return HashDeleteCoreAsync(key, fields, token);
        }

        public Task<KeyValuePair<string, byte[]>[]> HashGetAllAsync(string key, CancellationToken token = default)
        {
            return HashGetAllCoreAsync(key, token);
        }

        public Task<byte[]?> HashGetAsync(string key, string field, CancellationToken token = default)
        {
            return HashGetCoreAsync(key, field, token);
        }

        public Task<bool> HashSetAsync(string key, string field, byte[] value, CancellationToken token = default)
        {
            return HashSetCoreAsync(key, field, value, token);
        }

        public Task<HyperLogLogAddResult> HyperLogLogAddAsync(string key, byte[][] elements, CancellationToken token = default)
        {
            return HyperLogLogAddCoreAsync(key, elements, token);
        }

        public Task<HyperLogLogCountResult> HyperLogLogCountAsync(string[] keys, CancellationToken token = default)
        {
            return HyperLogLogCountCoreAsync(keys, token);
        }

        public Task<HyperLogLogMergeResult> HyperLogLogMergeAsync(string destinationKey, string[] sourceKeys, CancellationToken token = default)
        {
            return HyperLogLogMergeCoreAsync(destinationKey, sourceKeys, token);
        }

        public Task<long?> IncrByAsync(string key, long delta, CancellationToken token = default)
        {
            return IncrByCoreAsync(key, delta, token);
        }

        public Task<JsonArrayResult> JsonArrappendAsync(string key, string path, byte[][] values, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<JsonGetResult> JsonArrindexAsync(string key, string path, byte[] value, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<JsonArrayResult> JsonArrinsertAsync(string key, string path, int index, byte[][] values, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<JsonArrayResult> JsonArrlenAsync(string key, string[] paths, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<JsonArrayResult> JsonArrremAsync(string key, string path, int? index, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<JsonArrayResult> JsonArrtrimAsync(string key, string path, int start, int stop, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<JsonDelResult> JsonDelAsync(string key, string[] paths, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<JsonGetResult> JsonGetAsync(string key, string[] paths, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<JsonMGetResult> JsonMgetAsync(string[] keys, string path, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<JsonSetResult> JsonSetAsync(string key, string path, byte[] value, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<JsonArrayResult> JsonStrlenAsync(string key, string[] paths, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<JsonTypeResult> JsonTypeAsync(string key, string[] paths, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ListIndexResult> ListIndexAsync(string key, int index, CancellationToken token = default)
        {
            return ListIndexCoreAsync(key, index, token);
        }

        public Task<ListLengthResult> ListLengthAsync(string key, CancellationToken token = default)
        {
            return ListLengthCoreAsync(key, token);
        }

        public Task<ListPopResult> ListPopAsync(string key, bool left, CancellationToken token = default)
        {
            return ListPopCoreAsync(key, left, token);
        }

        public Task<ListPushResult> ListPushAsync(string key, byte[][] values, bool left, CancellationToken token = default)
        {
            return ListPushCoreAsync(key, values, left, token);
        }

        public Task<ListRangeResult> ListRangeAsync(string key, int start, int stop, CancellationToken token = default)
        {
            return ListRangeCoreAsync(key, start, stop, token);
        }

        public Task<ListSetResult> ListSetAsync(string key, int index, byte[] value, CancellationToken token = default)
        {
            return ListSetCoreAsync(key, index, value, token);
        }

        public Task<ListResultStatus> ListTrimAsync(string key, int start, int stop, CancellationToken token = default)
        {
            return ListTrimCoreAsync(key, start, stop, token);
        }

        public Task<bool> PExpireAsync(string key, TimeSpan expiration, CancellationToken token = default)
        {
            return ExpireAsync(key, expiration, token);
        }

        public Task<long> PttlAsync(string key, CancellationToken token = default)
        {
            return PttlCoreAsync(key, token);
        }

        public Task<SetCountResult> SetAddAsync(string key, byte[][] members, CancellationToken token = default)
        {
            return SetAddCoreAsync(key, members, token);
        }

        public Task<SetCountResult> SetCardinalityAsync(string key, CancellationToken token = default)
        {
            return SetCardinalityCoreAsync(key, token);
        }

        public Task<bool> SetManyAsync(KeyValuePair<string, byte[]>[] items, CancellationToken token = default)
        {
            return SetManyCoreAsync(items, token);
        }

        public Task<SetMembersResult> SetMembersAsync(string key, CancellationToken token = default)
        {
            return SetMembersCoreAsync(key, token);
        }

        public Task<SetCountResult> SetRemoveAsync(string key, byte[][] members, CancellationToken token = default)
        {
            return SetRemoveCoreAsync(key, members, token);
        }

        public Task<SortedSetCountResult> SortedSetAddAsync(string key, SortedSetEntry[] entries, CancellationToken token = default)
        {
            return SortedSetAddCoreAsync(key, entries, token);
        }

        public Task<SortedSetCountResult> SortedSetCardinalityAsync(string key, CancellationToken token = default)
        {
            return SortedSetCardinalityCoreAsync(key, token);
        }

        public Task<SortedSetCountResult> SortedSetCountByScoreAsync(string key, double minScore, double maxScore, CancellationToken token = default)
        {
            return SortedSetCountByScoreCoreAsync(key, minScore, maxScore, token);
        }

        public Task<SortedSetScoreResult> SortedSetIncrementAsync(string key, double increment, byte[] member, CancellationToken token = default)
        {
            return SortedSetIncrementCoreAsync(key, increment, member, token);
        }

        public Task<SortedSetRangeResult> SortedSetRangeAsync(string key, int start, int stop, CancellationToken token = default)
        {
            return SortedSetRangeCoreAsync(key, start, stop, token);
        }

        public Task<SortedSetRangeResult> SortedSetRangeByScoreAsync(string key, double minScore, double maxScore, CancellationToken token = default)
        {
            return SortedSetRangeByScoreCoreAsync(key, minScore, maxScore, token);
        }

        public Task<SortedSetRankResult> SortedSetRankAsync(string key, byte[] member, CancellationToken token = default)
        {
            return SortedSetRankCoreAsync(key, member, reverse: false, token);
        }

        public Task<SortedSetCountResult> SortedSetRemoveAsync(string key, byte[][] members, CancellationToken token = default)
        {
            return SortedSetRemoveCoreAsync(key, members, token);
        }

        public Task<SortedSetRemoveRangeResult> SortedSetRemoveRangeByScoreAsync(string key, double minScore, double maxScore, CancellationToken token = default)
        {
            return SortedSetRemoveRangeByScoreCoreAsync(key, minScore, maxScore, token);
        }

        public Task<SortedSetRankResult> SortedSetReverseRankAsync(string key, byte[] member, CancellationToken token = default)
        {
            return SortedSetRankCoreAsync(key, member, reverse: true, token);
        }

        public Task<SortedSetScoreResult> SortedSetScoreAsync(string key, byte[] member, CancellationToken token = default)
        {
            return SortedSetScoreCoreAsync(key, member, token);
        }

        public Task<StreamAckResult> StreamAckAsync(string key, string group, string[] ids, CancellationToken token = default)
        {
            return StreamAckCoreAsync(key, group, ids, token);
        }

        public Task<string?> StreamAddAsync(string key, string id, KeyValuePair<string, byte[]>[] fields, CancellationToken token = default)
        {
            return StreamAddCoreAsync(key, id, fields, token);
        }

        public Task<StreamClaimResult> StreamClaimAsync(string key, string group, string consumer, long minIdleTimeMs, string[] ids, long? idleMs = null, long? timeMs = null, long? retryCount = null, bool force = false, CancellationToken token = default)
        {
            return StreamClaimCoreAsync(key, group, consumer, minIdleTimeMs, ids, idleMs, timeMs, retryCount, force, token);
        }

        public Task<StreamConsumersInfoResult> StreamConsumersInfoAsync(string key, string group, CancellationToken token = default)
        {
            return StreamConsumersInfoCoreAsync(key, group, token);
        }

        public Task<long> StreamDeleteAsync(string key, string[] ids, CancellationToken token = default)
        {
            return StreamDeleteCoreAsync(key, ids, token);
        }

        public Task<StreamGroupCreateResult> StreamGroupCreateAsync(string key, string group, string startId, bool mkStream, CancellationToken token = default)
        {
            return StreamGroupCreateCoreAsync(key, group, startId, mkStream, token);
        }

        public Task<StreamGroupDelConsumerResult> StreamGroupDelConsumerAsync(string key, string group, string consumer, CancellationToken token = default)
        {
            return StreamGroupDelConsumerCoreAsync(key, group, consumer, token);
        }

        public Task<StreamGroupDestroyResult> StreamGroupDestroyAsync(string key, string group, CancellationToken token = default)
        {
            return StreamGroupDestroyCoreAsync(key, group, token);
        }

        public Task<StreamGroupReadResult> StreamGroupReadAsync(string group, string consumer, string[] keys, string[] ids, int? count, TimeSpan? block, CancellationToken token = default)
        {
            return StreamGroupReadCoreAsync(group, consumer, keys, ids, count, block, token);
        }

        public Task<StreamGroupSetIdResultStatus> StreamGroupSetIdAsync(string key, string group, string lastId, CancellationToken token = default)
        {
            return StreamGroupSetIdCoreAsync(key, group, lastId, token);
        }

        public Task<StreamGroupsInfoResult> StreamGroupsInfoAsync(string key, CancellationToken token = default)
        {
            return StreamGroupsInfoCoreAsync(key, token);
        }

        public Task<StreamInfoResult> StreamInfoAsync(string key, CancellationToken token = default)
        {
            return StreamInfoCoreAsync(key, token);
        }

        public Task<string?> StreamLastIdAsync(string key, CancellationToken token = default)
        {
            return StreamLastIdCoreAsync(key, token);
        }

        public Task<long> StreamLengthAsync(string key, CancellationToken token = default)
        {
            return StreamLengthCoreAsync(key, token);
        }

        public Task<StreamPendingResult> StreamPendingAsync(string key, string group, long? minIdleTimeMs = null, string? start = null, string? end = null, int? count = null, string? consumer = null, CancellationToken token = default)
        {
            return StreamPendingCoreAsync(key, group, minIdleTimeMs, start, end, count, consumer, token);
        }

        public Task<StreamEntry[]> StreamRangeAsync(string key, string start, string end, int? count, CancellationToken token = default)
        {
            return StreamRangeCoreAsync(key, start, end, count, reverse: false, token);
        }

        public Task<StreamEntry[]> StreamRangeReverseAsync(string key, string start, string end, int? count, CancellationToken token = default)
        {
            return StreamRangeCoreAsync(key, start, end, count, reverse: true, token);
        }

        public Task<StreamReadResult[]> StreamReadAsync(string[] keys, string[] ids, int? count, CancellationToken token = default)
        {
            return StreamReadCoreAsync(keys, ids, count, token);
        }

        public Task<StreamSetIdResultStatus> StreamSetIdAsync(string key, string lastId, CancellationToken token = default)
        {
            return StreamSetIdCoreAsync(key, lastId, token);
        }

        public Task<long> StreamTrimAsync(string key, int? maxLength = null, string? minId = null, bool approximate = false, CancellationToken token = default)
        {
            return StreamTrimCoreAsync(key, maxLength, minId, approximate, token);
        }

        public Task<ProbabilisticResultStatus> TDigestAddAsync(string key, double[] values, CancellationToken token = default)
        {
            return TDigestAddCoreAsync(key, values, token);
        }

        public Task<ProbabilisticDoubleArrayResult> TDigestByRankAsync(string key, long[] ranks, CancellationToken token = default)
        {
            return TDigestByRankCoreAsync(key, ranks, reverse: false, token);
        }

        public Task<ProbabilisticDoubleArrayResult> TDigestByRevRankAsync(string key, long[] ranks, CancellationToken token = default)
        {
            return TDigestByRankCoreAsync(key, ranks, reverse: true, token);
        }

        public Task<ProbabilisticDoubleArrayResult> TDigestCdfAsync(string key, double[] values, CancellationToken token = default)
        {
            return TDigestCdfCoreAsync(key, values, token);
        }

        public Task<ProbabilisticResultStatus> TDigestCreateAsync(string key, int compression, CancellationToken token = default)
        {
            return TDigestCreateCoreAsync(key, compression, token);
        }

        public Task<ProbabilisticInfoResult> TDigestInfoAsync(string key, CancellationToken token = default)
        {
            return TDigestInfoCoreAsync(key, token);
        }

        public Task<ProbabilisticDoubleResult> TDigestMaxAsync(string key, CancellationToken token = default)
        {
            return TDigestMinMaxCoreAsync(key, max: true, token);
        }

        public Task<ProbabilisticDoubleResult> TDigestMinAsync(string key, CancellationToken token = default)
        {
            return TDigestMinMaxCoreAsync(key, max: false, token);
        }

        public Task<ProbabilisticDoubleArrayResult> TDigestQuantileAsync(string key, double[] quantiles, CancellationToken token = default)
        {
            return TDigestQuantileCoreAsync(key, quantiles, token);
        }

        public Task<ProbabilisticArrayResult> TDigestRankAsync(string key, double[] values, CancellationToken token = default)
        {
            return TDigestRankCoreAsync(key, values, reverse: false, token);
        }

        public Task<ProbabilisticResultStatus> TDigestResetAsync(string key, CancellationToken token = default)
        {
            return TDigestResetCoreAsync(key, token);
        }

        public Task<ProbabilisticArrayResult> TDigestRevRankAsync(string key, double[] values, CancellationToken token = default)
        {
            return TDigestRankCoreAsync(key, values, reverse: true, token);
        }

        public Task<ProbabilisticDoubleResult> TDigestTrimmedMeanAsync(string key, double lowerQuantile, double upperQuantile, CancellationToken token = default)
        {
            return TDigestTrimmedMeanCoreAsync(key, lowerQuantile, upperQuantile, token);
        }

        public Task<TimeSeriesAddResult> TimeSeriesAddAsync(string key, long timestamp, double value, TimeSeriesDuplicatePolicy? onDuplicate, bool createIfMissing, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<TimeSeriesResultStatus> TimeSeriesCreateAsync(string key, long? retentionTimeMs, TimeSeriesDuplicatePolicy? duplicatePolicy, KeyValuePair<string, string>[]? labels, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<TimeSeriesDeleteResult> TimeSeriesDeleteAsync(string key, long fromTimestamp, long toTimestamp, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<TimeSeriesGetResult> TimeSeriesGetAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<TimeSeriesAddResult> TimeSeriesIncrementByAsync(string key, double increment, long? timestamp, bool createIfMissing, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<TimeSeriesInfoResult> TimeSeriesInfoAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<TimeSeriesMRangeResult> TimeSeriesMultiRangeAsync(long fromTimestamp, long toTimestamp, bool reverse, int? count, string? aggregationType, long? bucketDurationMs, KeyValuePair<string, string>[] filters, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<TimeSeriesRangeResult> TimeSeriesRangeAsync(string key, long fromTimestamp, long toTimestamp, bool reverse, int? count, string? aggregationType, long? bucketDurationMs, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticStringArrayResult> TopKAddAsync(string key, byte[][] items, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticArrayResult> TopKCountAsync(string key, byte[][] items, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticStringArrayResult> TopKIncrByAsync(string key, KeyValuePair<byte[], long>[] increments, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticInfoResult> TopKInfoAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticStringArrayResult> TopKListAsync(string key, bool withCount, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticArrayResult> TopKQueryAsync(string key, byte[][] items, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticResultStatus> TopKReserveAsync(string key, int k, int width, int depth, double decay, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<long> TtlAsync(string key, CancellationToken token = default)
        {
            return TtlCoreAsync(key, token);
        }

        public Task<VectorDeleteResult> VectorDeleteAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<VectorGetResult> VectorGetAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<VectorSearchResult> VectorSearchAsync(string keyPrefix, int topK, int offset, string metric, double[] queryVector, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<VectorSetResult> VectorSetAsync(string key, double[] vector, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<VectorSimilarityResult> VectorSimilarityAsync(string key, string otherKey, string metric, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<VectorSizeResult> VectorSizeAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        private async Task<long> CleanUpExpiredKeysCoreAsync(CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            var filter = Builders<KeyValueDocument>.Filter.Lte(x => x.ExpireAt, nowUtc);
            var result = await collection.DeleteManyAsync(filter, token);
            var hashFilter = Builders<HashDocument>.Filter.Lte(x => x.ExpireAt, nowUtc);
            var hashResult = await hashCollection.DeleteManyAsync(hashFilter, token);
            var listFilter = Builders<ListDocument>.Filter.Lte(x => x.ExpireAt, nowUtc);
            var listResult = await listCollection.DeleteManyAsync(listFilter, token);
            var setFilter = Builders<SetDocument>.Filter.Lte(x => x.ExpireAt, nowUtc);
            var setResult = await setCollection.DeleteManyAsync(setFilter, token);
            var sortedSetFilter = Builders<SortedSetDocument>.Filter.Lte(x => x.ExpireAt, nowUtc);
            var sortedSetResult = await sortedSetCollection.DeleteManyAsync(sortedSetFilter, token);
            var streamFilter = Builders<StreamDocument>.Filter.Lte(x => x.ExpireAt, nowUtc);
            var streamResult = await streamCollection.DeleteManyAsync(streamFilter, token);
            var hllFilter = Builders<HyperLogLogDocument>.Filter.Lte(x => x.ExpireAt, nowUtc);
            var hllResult = await hyperLogLogCollection.DeleteManyAsync(hllFilter, token);
            var bloomFilter = Builders<BloomDocument>.Filter.Lte(x => x.ExpireAt, nowUtc);
            var bloomResult = await bloomCollection.DeleteManyAsync(bloomFilter, token);
            var cuckooFilter = Builders<CuckooDocument>.Filter.Lte(x => x.ExpireAt, nowUtc);
            var cuckooResult = await cuckooCollection.DeleteManyAsync(cuckooFilter, token);
            var tdigestFilter = Builders<TDigestDocument>.Filter.Lte(x => x.ExpireAt, nowUtc);
            var tdigestResult = await tdigestCollection.DeleteManyAsync(tdigestFilter, token);
            return result.DeletedCount + hashResult.DeletedCount + listResult.DeletedCount + setResult.DeletedCount + sortedSetResult.DeletedCount + streamResult.DeletedCount + hllResult.DeletedCount + bloomResult.DeletedCount + cuckooResult.DeletedCount + tdigestResult.DeletedCount;
        }

        private async Task<byte[]?[]> GetManyCoreAsync(string[] keys, CancellationToken token)
        {
            if (keys.Length == 0)
            {
                return Array.Empty<byte[]?>();
            }

            var nowUtc = DateTime.UtcNow;
            var filter = ActiveKeysFilter(keys, nowUtc);
            var docs = await collection.Find(filter).ToListAsync(token);
            var byKey = docs
                .GroupBy(x => x.Key, StringComparer.Ordinal)
                .ToDictionary(x => x.Key, x => x.Last().Value, StringComparer.Ordinal);

            var output = new byte[]?[keys.Length];
            for (int i = 0; i < keys.Length; i++)
            {
                output[i] = byKey.TryGetValue(keys[i], out var value) ? value : null;
            }

            return output;
        }

        private async Task<bool> SetManyCoreAsync(KeyValuePair<string, byte[]>[] items, CancellationToken token)
        {
            if (items.Length == 0)
            {
                return true;
            }

            var writes = new List<WriteModel<KeyValueDocument>>(items.Length);
            foreach (var item in items)
            {
                var filter = KeyFilter(item.Key);
                var update = Builders<KeyValueDocument>.Update
                    .Set(x => x.Key, item.Key)
                    .Set(x => x.Value, item.Value)
                    .Set(x => x.ExpireAt, (DateTime?)null);
                writes.Add(new UpdateOneModel<KeyValueDocument>(filter, update) { IsUpsert = true });
            }

            await collection.BulkWriteAsync(writes, new BulkWriteOptions { IsOrdered = false }, token);
            return true;
        }

        private async Task<long> PttlCoreAsync(string key, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            var doc = await collection.Find(KeyFilter(key)).FirstOrDefaultAsync(token);
            if (doc != null)
            {
                if (!doc.ExpireAt.HasValue)
                {
                    return -1;
                }

                if (doc.ExpireAt.Value <= nowUtc)
                {
                    await collection.DeleteOneAsync(KeyFilter(key), token);
                    return -2;
                }

                return (long)(doc.ExpireAt.Value - nowUtc).TotalMilliseconds;
            }

            var hashDoc = await hashCollection.Find(HashKeyFilter(key)).FirstOrDefaultAsync(token);
            if (hashDoc != null)
            {
                if (!hashDoc.ExpireAt.HasValue)
                {
                    return -1;
                }

                if (hashDoc.ExpireAt.Value <= nowUtc)
                {
                    await hashCollection.DeleteOneAsync(HashKeyFilter(key), token);
                    return -2;
                }

                return (long)(hashDoc.ExpireAt.Value - nowUtc).TotalMilliseconds;
            }

            var listDoc = await listCollection.Find(ListKeyFilter(key)).FirstOrDefaultAsync(token);
            if (listDoc != null)
            {
                if (!listDoc.ExpireAt.HasValue)
                {
                    return -1;
                }

                if (listDoc.ExpireAt.Value <= nowUtc)
                {
                    await listCollection.DeleteOneAsync(ListKeyFilter(key), token);
                    return -2;
                }

                return (long)(listDoc.ExpireAt.Value - nowUtc).TotalMilliseconds;
            }

            var setDoc = await setCollection.Find(SetKeyFilter(key)).FirstOrDefaultAsync(token);
            if (setDoc != null)
            {
                if (!setDoc.ExpireAt.HasValue)
                {
                    return -1;
                }

                if (setDoc.ExpireAt.Value <= nowUtc)
                {
                    await setCollection.DeleteOneAsync(SetKeyFilter(key), token);
                    return -2;
                }

                return (long)(setDoc.ExpireAt.Value - nowUtc).TotalMilliseconds;
            }

            var sortedSetDoc = await sortedSetCollection.Find(SortedSetKeyFilter(key)).FirstOrDefaultAsync(token);
            if (sortedSetDoc != null)
            {
                if (!sortedSetDoc.ExpireAt.HasValue)
                {
                    return -1;
                }

                if (sortedSetDoc.ExpireAt.Value <= nowUtc)
                {
                    await sortedSetCollection.DeleteOneAsync(SortedSetKeyFilter(key), token);
                    return -2;
                }

                return (long)(sortedSetDoc.ExpireAt.Value - nowUtc).TotalMilliseconds;
            }

            var streamDoc = await streamCollection.Find(StreamKeyFilter(key)).FirstOrDefaultAsync(token);
            if (streamDoc != null)
            {
                if (!streamDoc.ExpireAt.HasValue)
                {
                    return -1;
                }

                if (streamDoc.ExpireAt.Value <= nowUtc)
                {
                    await streamCollection.DeleteOneAsync(StreamKeyFilter(key), token);
                    return -2;
                }

                return (long)(streamDoc.ExpireAt.Value - nowUtc).TotalMilliseconds;
            }

            var hllDoc = await hyperLogLogCollection.Find(HyperLogLogKeyFilter(key)).FirstOrDefaultAsync(token);
            if (hllDoc != null)
            {
                if (!hllDoc.ExpireAt.HasValue)
                {
                    return -1;
                }

                if (hllDoc.ExpireAt.Value <= nowUtc)
                {
                    await hyperLogLogCollection.DeleteOneAsync(HyperLogLogKeyFilter(key), token);
                    return -2;
                }

                return (long)(hllDoc.ExpireAt.Value - nowUtc).TotalMilliseconds;
            }

            var bloomDoc = await bloomCollection.Find(BloomKeyFilter(key)).FirstOrDefaultAsync(token);
            if (bloomDoc != null)
            {
                if (!bloomDoc.ExpireAt.HasValue)
                {
                    return -1;
                }

                if (bloomDoc.ExpireAt.Value <= nowUtc)
                {
                    await bloomCollection.DeleteOneAsync(BloomKeyFilter(key), token);
                    return -2;
                }

                return (long)(bloomDoc.ExpireAt.Value - nowUtc).TotalMilliseconds;
            }

            var cuckooDoc = await cuckooCollection.Find(CuckooKeyFilter(key)).FirstOrDefaultAsync(token);
            if (cuckooDoc != null)
            {
                if (!cuckooDoc.ExpireAt.HasValue)
                {
                    return -1;
                }

                if (cuckooDoc.ExpireAt.Value <= nowUtc)
                {
                    await cuckooCollection.DeleteOneAsync(CuckooKeyFilter(key), token);
                    return -2;
                }

                return (long)(cuckooDoc.ExpireAt.Value - nowUtc).TotalMilliseconds;
            }

            var tdigestDoc = await tdigestCollection.Find(TDigestKeyFilter(key)).FirstOrDefaultAsync(token);
            if (tdigestDoc != null)
            {
                if (!tdigestDoc.ExpireAt.HasValue)
                {
                    return -1;
                }

                if (tdigestDoc.ExpireAt.Value <= nowUtc)
                {
                    await tdigestCollection.DeleteOneAsync(TDigestKeyFilter(key), token);
                    return -2;
                }

                return (long)(tdigestDoc.ExpireAt.Value - nowUtc).TotalMilliseconds;
            }

            return -2;
        }

        private async Task<long> TtlCoreAsync(string key, CancellationToken token)
        {
            var pttl = await PttlCoreAsync(key, token);
            if (pttl < 0)
            {
                return pttl;
            }

            return pttl / 1000;
        }

        private async Task<long?> IncrByCoreAsync(string key, long delta, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await hashCollection.CountDocumentsAsync(ActiveHashKeyFilter(key, nowUtc), null, token) > 0 ||
                await listCollection.CountDocumentsAsync(ActiveListKeyFilter(key, nowUtc), null, token) > 0 ||
                await setCollection.CountDocumentsAsync(ActiveSetKeyFilter(key, nowUtc), null, token) > 0 ||
                await sortedSetCollection.CountDocumentsAsync(ActiveSortedSetKeyFilter(key, nowUtc), null, token) > 0 ||
                await streamCollection.CountDocumentsAsync(ActiveStreamKeyFilter(key, nowUtc), null, token) > 0 ||
                await hyperLogLogCollection.CountDocumentsAsync(ActiveHyperLogLogKeyFilter(key, nowUtc), null, token) > 0 ||
                await bloomCollection.CountDocumentsAsync(ActiveBloomKeyFilter(key, nowUtc), null, token) > 0 ||
                await cuckooCollection.CountDocumentsAsync(ActiveCuckooKeyFilter(key, nowUtc), null, token) > 0 ||
                await tdigestCollection.CountDocumentsAsync(ActiveTDigestKeyFilter(key, nowUtc), null, token) > 0)
            {
                return null;
            }

            for (int attempt = 0; attempt < 5; attempt++)
            {
                token.ThrowIfCancellationRequested();
                nowUtc = DateTime.UtcNow;
                var current = await collection.Find(ActiveKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
                if (current == null)
                {
                    var initialBytes = Encoding.UTF8.GetBytes(delta.ToString(CultureInfo.InvariantCulture));
                    try
                    {
                        await collection.InsertOneAsync(
                            new KeyValueDocument { Key = key, Value = initialBytes, ExpireAt = null },
                            null,
                            token);
                        return delta;
                    }
                    catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
                    {
                        continue;
                    }
                }

                var currentText = Encoding.UTF8.GetString(current.Value);
                if (!long.TryParse(currentText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var currentValue))
                {
                    return null;
                }

                long nextValue;
                try
                {
                    nextValue = checked(currentValue + delta);
                }
                catch (OverflowException)
                {
                    return null;
                }

                var nextBytes = Encoding.UTF8.GetBytes(nextValue.ToString(CultureInfo.InvariantCulture));
                var filter = Builders<KeyValueDocument>.Filter.And(
                    Builders<KeyValueDocument>.Filter.Eq(x => x.Id, current.Id),
                    Builders<KeyValueDocument>.Filter.Eq(x => x.Value, current.Value));
                var update = Builders<KeyValueDocument>.Update.Set(x => x.Value, nextBytes);
                var result = await collection.UpdateOneAsync(filter, update, null, token);
                if (result.ModifiedCount > 0 || result.MatchedCount > 0)
                {
                    return nextValue;
                }
            }

            return null;
        }

        private async Task<bool> HashSetCoreAsync(string key, string field, byte[] value, CancellationToken token)
        {
            for (int attempt = 0; attempt < 5; attempt++)
            {
                token.ThrowIfCancellationRequested();
                var nowUtc = DateTime.UtcNow;
                var existing = await hashCollection.Find(ActiveHashKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
                if (existing == null)
                {
                    try
                    {
                        var created = new HashDocument
                        {
                            Key = key,
                            ExpireAt = null,
                            Fields = new List<HashFieldDocument>
                            {
                                new HashFieldDocument { Field = field, Value = value }
                            }
                        };
                        await hashCollection.InsertOneAsync(created, null, token);
                        return true;
                    }
                    catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
                    {
                        continue;
                    }
                }

                var fieldIndex = existing.Fields.FindIndex(x => string.Equals(x.Field, field, StringComparison.Ordinal));
                var added = fieldIndex < 0;
                if (added)
                {
                    existing.Fields.Add(new HashFieldDocument { Field = field, Value = value });
                }
                else
                {
                    existing.Fields[fieldIndex].Value = value;
                }

                var result = await hashCollection.ReplaceOneAsync(
                    Builders<HashDocument>.Filter.Eq(x => x.Id, existing.Id),
                    existing,
                    new ReplaceOptions(),
                    token);

                if (result.MatchedCount > 0)
                {
                    return added;
                }
            }

            return false;
        }

        private async Task<byte[]?> HashGetCoreAsync(string key, string field, CancellationToken token)
        {
            var doc = await hashCollection.Find(ActiveHashKeyFilter(key, DateTime.UtcNow)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return null;
            }

            var item = doc.Fields.FirstOrDefault(x => string.Equals(x.Field, field, StringComparison.Ordinal));
            return item?.Value;
        }

        private async Task<long> HashDeleteCoreAsync(string key, string[] fields, CancellationToken token)
        {
            if (fields.Length == 0)
            {
                return 0;
            }

            var doc = await hashCollection.Find(ActiveHashKeyFilter(key, DateTime.UtcNow)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return 0;
            }

            var fieldSet = new HashSet<string>(fields, StringComparer.Ordinal);
            var originalCount = doc.Fields.Count;
            doc.Fields.RemoveAll(x => fieldSet.Contains(x.Field));
            var removed = originalCount - doc.Fields.Count;
            if (removed <= 0)
            {
                return 0;
            }

            if (doc.Fields.Count == 0)
            {
                await hashCollection.DeleteOneAsync(Builders<HashDocument>.Filter.Eq(x => x.Id, doc.Id), token);
            }
            else
            {
                await hashCollection.ReplaceOneAsync(Builders<HashDocument>.Filter.Eq(x => x.Id, doc.Id), doc, new ReplaceOptions(), token);
            }

            return removed;
        }

        private async Task<KeyValuePair<string, byte[]>[]> HashGetAllCoreAsync(string key, CancellationToken token)
        {
            var doc = await hashCollection.Find(ActiveHashKeyFilter(key, DateTime.UtcNow)).FirstOrDefaultAsync(token);
            if (doc == null || doc.Fields.Count == 0)
            {
                return Array.Empty<KeyValuePair<string, byte[]>>();
            }

            var output = new KeyValuePair<string, byte[]>[doc.Fields.Count];
            for (int i = 0; i < doc.Fields.Count; i++)
            {
                output[i] = new KeyValuePair<string, byte[]>(doc.Fields[i].Field, doc.Fields[i].Value);
            }

            return output;
        }

        private async Task<bool> IsListWrongTypeAsync(string key, DateTime nowUtc, CancellationToken token)
        {
            var stringCount = await collection.CountDocumentsAsync(ActiveKeyFilter(key, nowUtc), null, token);
            if (stringCount > 0)
            {
                return true;
            }

            var hashCount = await hashCollection.CountDocumentsAsync(ActiveHashKeyFilter(key, nowUtc), null, token);
            if (hashCount > 0)
            {
                return true;
            }

            var setCount = await setCollection.CountDocumentsAsync(ActiveSetKeyFilter(key, nowUtc), null, token);
            if (setCount > 0)
            {
                return true;
            }

            var sortedSetCount = await sortedSetCollection.CountDocumentsAsync(ActiveSortedSetKeyFilter(key, nowUtc), null, token);
            if (sortedSetCount > 0)
            {
                return true;
            }

            var streamCount = await streamCollection.CountDocumentsAsync(ActiveStreamKeyFilter(key, nowUtc), null, token);
            if (streamCount > 0)
            {
                return true;
            }

            var hllCount = await hyperLogLogCollection.CountDocumentsAsync(ActiveHyperLogLogKeyFilter(key, nowUtc), null, token);
            if (hllCount > 0)
            {
                return true;
            }

            if (await bloomCollection.CountDocumentsAsync(ActiveBloomKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await cuckooCollection.CountDocumentsAsync(ActiveCuckooKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            return await tdigestCollection.CountDocumentsAsync(ActiveTDigestKeyFilter(key, nowUtc), null, token) > 0;
        }

        private static string ToMemberKey(byte[] member) => Convert.ToBase64String(member);

        private static ulong ToUInt64(byte[] data, int offset)
        {
            return ((ulong)data[offset] << 56) |
                   ((ulong)data[offset + 1] << 48) |
                   ((ulong)data[offset + 2] << 40) |
                   ((ulong)data[offset + 3] << 32) |
                   ((ulong)data[offset + 4] << 24) |
                   ((ulong)data[offset + 5] << 16) |
                   ((ulong)data[offset + 6] << 8) |
                   data[offset + 7];
        }

        private static void GetHashPair(byte[] value, out ulong hash1, out ulong hash2)
        {
            var digest = SHA256.HashData(value);
            hash1 = ToUInt64(digest, 0);
            hash2 = ToUInt64(digest, 8);
            if (hash2 == 0)
            {
                hash2 = 0x9e3779b97f4a7c15UL;
            }
        }

        private static bool TryComputeBloomSizing(double errorRate, long capacity, out long bitSize, out int hashFunctions)
        {
            bitSize = 0;
            hashFunctions = 0;
            if (capacity <= 0 || errorRate <= 0 || errorRate >= 1)
            {
                return false;
            }

            var ln2 = Math.Log(2);
            var m = -(capacity * Math.Log(errorRate)) / (ln2 * ln2);
            if (double.IsNaN(m) || double.IsInfinity(m) || m <= 0)
            {
                return false;
            }

            bitSize = Math.Max(64, (long)Math.Ceiling(m));
            var k = (bitSize / (double)capacity) * ln2;
            hashFunctions = Math.Max(1, (int)Math.Round(k));
            return true;
        }

        private static bool SetBloomBitsAndReturnChanged(BloomDocument doc, byte[] value)
        {
            GetHashPair(value, out var hash1, out var hash2);
            var changed = false;
            for (int i = 0; i < doc.HashFunctions; i++)
            {
                var bitIndex = (long)((hash1 + ((ulong)i * hash2)) % (ulong)doc.BitSize);
                var byteIndex = (int)(bitIndex / 8);
                var mask = (byte)(1 << (int)(bitIndex % 8));
                if ((doc.Bits[byteIndex] & mask) == 0)
                {
                    doc.Bits[byteIndex] |= mask;
                    changed = true;
                }
            }

            return changed;
        }

        private static bool BloomMayContain(BloomDocument doc, byte[] value)
        {
            GetHashPair(value, out var hash1, out var hash2);
            for (int i = 0; i < doc.HashFunctions; i++)
            {
                var bitIndex = (long)((hash1 + ((ulong)i * hash2)) % (ulong)doc.BitSize);
                var byteIndex = (int)(bitIndex / 8);
                var mask = (byte)(1 << (int)(bitIndex % 8));
                if ((doc.Bits[byteIndex] & mask) == 0)
                {
                    return false;
                }
            }

            return true;
        }

        private static List<SortedSetMemberDocument> SortSortedSetMembers(IEnumerable<SortedSetMemberDocument> members)
        {
            return members
                .OrderBy(x => x.Score)
                .ThenBy(x => x.MemberKey, StringComparer.Ordinal)
                .ToList();
        }

        private async Task<bool> IsSetWrongTypeAsync(string key, DateTime nowUtc, CancellationToken token)
        {
            if (await collection.CountDocumentsAsync(ActiveKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await hashCollection.CountDocumentsAsync(ActiveHashKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await listCollection.CountDocumentsAsync(ActiveListKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await streamCollection.CountDocumentsAsync(ActiveStreamKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await hyperLogLogCollection.CountDocumentsAsync(ActiveHyperLogLogKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await bloomCollection.CountDocumentsAsync(ActiveBloomKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await cuckooCollection.CountDocumentsAsync(ActiveCuckooKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await sortedSetCollection.CountDocumentsAsync(ActiveSortedSetKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            return await tdigestCollection.CountDocumentsAsync(ActiveTDigestKeyFilter(key, nowUtc), null, token) > 0;
        }

        private async Task<bool> IsSortedSetWrongTypeAsync(string key, DateTime nowUtc, CancellationToken token)
        {
            if (await collection.CountDocumentsAsync(ActiveKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await hashCollection.CountDocumentsAsync(ActiveHashKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await listCollection.CountDocumentsAsync(ActiveListKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await streamCollection.CountDocumentsAsync(ActiveStreamKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await hyperLogLogCollection.CountDocumentsAsync(ActiveHyperLogLogKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await bloomCollection.CountDocumentsAsync(ActiveBloomKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await cuckooCollection.CountDocumentsAsync(ActiveCuckooKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await setCollection.CountDocumentsAsync(ActiveSetKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            return await tdigestCollection.CountDocumentsAsync(ActiveTDigestKeyFilter(key, nowUtc), null, token) > 0;
        }

        private static bool TryParseStreamId(string id, out long milliseconds, out long sequence)
        {
            milliseconds = 0;
            sequence = 0;
            var sepIndex = id.IndexOf('-');
            if (sepIndex <= 0 || sepIndex >= id.Length - 1)
            {
                return false;
            }

            var msPart = id.Substring(0, sepIndex);
            var seqPart = id.Substring(sepIndex + 1);
            if (!long.TryParse(msPart, NumberStyles.Integer, CultureInfo.InvariantCulture, out milliseconds) || milliseconds < 0)
            {
                return false;
            }

            if (!long.TryParse(seqPart, NumberStyles.Integer, CultureInfo.InvariantCulture, out sequence) || sequence < 0)
            {
                return false;
            }

            return true;
        }

        private static int CompareStreamIds(long leftMs, long leftSeq, long rightMs, long rightSeq)
        {
            var msCompare = leftMs.CompareTo(rightMs);
            if (msCompare != 0)
            {
                return msCompare;
            }

            return leftSeq.CompareTo(rightSeq);
        }

        private static StreamEntry ToStreamEntry(StreamEntryDocument entry)
        {
            var fields = entry.Fields
                .Select(x => new KeyValuePair<string, byte[]>(x.Field, x.Value))
                .ToArray();
            return new StreamEntry(entry.Id, fields);
        }

        private static int CompareStreamIds(string left, string right)
        {
            if (TryParseStreamId(left, out var leftMs, out var leftSeq) && TryParseStreamId(right, out var rightMs, out var rightSeq))
            {
                return CompareStreamIds(leftMs, leftSeq, rightMs, rightSeq);
            }

            return string.CompareOrdinal(left, right);
        }

        private static string ResolveGroupStartId(StreamDocument? doc, string startId)
        {
            if (startId == "$")
            {
                return doc?.LastId ?? "0-0";
            }

            if (startId == "-")
            {
                return "0-0";
            }

            return startId;
        }

        private static bool IsValidGroupStartId(StreamDocument? doc, string startId)
        {
            var resolved = ResolveGroupStartId(doc, startId);
            return TryParseStreamId(resolved, out _, out _);
        }

        private static void EnsureConsumer(StreamGroupDocument group, string consumer, long nowUnixMs)
        {
            var existing = group.Consumers.FirstOrDefault(x => string.Equals(x.Name, consumer, StringComparison.Ordinal));
            if (existing == null)
            {
                group.Consumers.Add(new StreamConsumerDocument { Name = consumer, LastSeenUnixMs = nowUnixMs });
                return;
            }

            existing.LastSeenUnixMs = nowUnixMs;
        }

        private static void RemovePendingForMissingEntries(StreamDocument doc)
        {
            var validIds = new HashSet<string>(doc.Entries.Select(x => x.Id), StringComparer.Ordinal);
            for (int i = 0; i < doc.Groups.Count; i++)
            {
                doc.Groups[i].Pending.RemoveAll(x => !validIds.Contains(x.Id));
            }
        }

        private async Task<bool> IsStreamWrongTypeAsync(string key, DateTime nowUtc, CancellationToken token)
        {
            if (await collection.CountDocumentsAsync(ActiveKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await hashCollection.CountDocumentsAsync(ActiveHashKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await listCollection.CountDocumentsAsync(ActiveListKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await setCollection.CountDocumentsAsync(ActiveSetKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await hyperLogLogCollection.CountDocumentsAsync(ActiveHyperLogLogKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await bloomCollection.CountDocumentsAsync(ActiveBloomKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await cuckooCollection.CountDocumentsAsync(ActiveCuckooKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await sortedSetCollection.CountDocumentsAsync(ActiveSortedSetKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            return await tdigestCollection.CountDocumentsAsync(ActiveTDigestKeyFilter(key, nowUtc), null, token) > 0;
        }

        private async Task<bool> IsHyperLogLogWrongTypeAsync(string key, DateTime nowUtc, CancellationToken token)
        {
            if (await collection.CountDocumentsAsync(ActiveKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await hashCollection.CountDocumentsAsync(ActiveHashKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await listCollection.CountDocumentsAsync(ActiveListKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await setCollection.CountDocumentsAsync(ActiveSetKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await sortedSetCollection.CountDocumentsAsync(ActiveSortedSetKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await bloomCollection.CountDocumentsAsync(ActiveBloomKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await cuckooCollection.CountDocumentsAsync(ActiveCuckooKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await streamCollection.CountDocumentsAsync(ActiveStreamKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            return await tdigestCollection.CountDocumentsAsync(ActiveTDigestKeyFilter(key, nowUtc), null, token) > 0;
        }

        private async Task<bool> IsBloomWrongTypeAsync(string key, DateTime nowUtc, CancellationToken token)
        {
            if (await collection.CountDocumentsAsync(ActiveKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await hashCollection.CountDocumentsAsync(ActiveHashKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await listCollection.CountDocumentsAsync(ActiveListKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await setCollection.CountDocumentsAsync(ActiveSetKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await sortedSetCollection.CountDocumentsAsync(ActiveSortedSetKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await streamCollection.CountDocumentsAsync(ActiveStreamKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await hyperLogLogCollection.CountDocumentsAsync(ActiveHyperLogLogKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await cuckooCollection.CountDocumentsAsync(ActiveCuckooKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            return await tdigestCollection.CountDocumentsAsync(ActiveTDigestKeyFilter(key, nowUtc), null, token) > 0;
        }

        private async Task<bool> IsCuckooWrongTypeAsync(string key, DateTime nowUtc, CancellationToken token)
        {
            if (await collection.CountDocumentsAsync(ActiveKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await hashCollection.CountDocumentsAsync(ActiveHashKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await listCollection.CountDocumentsAsync(ActiveListKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await setCollection.CountDocumentsAsync(ActiveSetKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await sortedSetCollection.CountDocumentsAsync(ActiveSortedSetKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await streamCollection.CountDocumentsAsync(ActiveStreamKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await hyperLogLogCollection.CountDocumentsAsync(ActiveHyperLogLogKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await bloomCollection.CountDocumentsAsync(ActiveBloomKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            return await tdigestCollection.CountDocumentsAsync(ActiveTDigestKeyFilter(key, nowUtc), null, token) > 0;
        }

        private async Task<bool> IsTDigestWrongTypeAsync(string key, DateTime nowUtc, CancellationToken token)
        {
            if (await collection.CountDocumentsAsync(ActiveKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await hashCollection.CountDocumentsAsync(ActiveHashKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await listCollection.CountDocumentsAsync(ActiveListKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await setCollection.CountDocumentsAsync(ActiveSetKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await sortedSetCollection.CountDocumentsAsync(ActiveSortedSetKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await streamCollection.CountDocumentsAsync(ActiveStreamKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await hyperLogLogCollection.CountDocumentsAsync(ActiveHyperLogLogKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            if (await bloomCollection.CountDocumentsAsync(ActiveBloomKeyFilter(key, nowUtc), null, token) > 0)
            {
                return true;
            }

            return await cuckooCollection.CountDocumentsAsync(ActiveCuckooKeyFilter(key, nowUtc), null, token) > 0;
        }

        private async Task<HyperLogLogAddResult> HyperLogLogAddCoreAsync(string key, byte[][] elements, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsHyperLogLogWrongTypeAsync(key, nowUtc, token))
            {
                return new HyperLogLogAddResult(HyperLogLogResultStatus.WrongType, false);
            }

            if (elements.Length == 0)
            {
                return new HyperLogLogAddResult(HyperLogLogResultStatus.Ok, false);
            }

            for (int attempt = 0; attempt < 5; attempt++)
            {
                token.ThrowIfCancellationRequested();
                nowUtc = DateTime.UtcNow;
                var doc = await hyperLogLogCollection.Find(ActiveHyperLogLogKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
                if (doc == null)
                {
                    try
                    {
                        var unique = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                        for (int i = 0; i < elements.Length; i++)
                        {
                            unique[ToMemberKey(elements[i])] = elements[i];
                        }

                        var created = new HyperLogLogDocument
                        {
                            Key = key,
                            ExpireAt = null,
                            Members = unique.Select(x => new HyperLogLogMemberDocument { MemberKey = x.Key, Member = x.Value }).ToList()
                        };

                        await hyperLogLogCollection.InsertOneAsync(created, null, token);
                        return new HyperLogLogAddResult(HyperLogLogResultStatus.Ok, created.Members.Count > 0);
                    }
                    catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
                    {
                        continue;
                    }
                }

                var existing = new HashSet<string>(doc.Members.Select(x => x.MemberKey), StringComparer.Ordinal);
                var changed = false;
                for (int i = 0; i < elements.Length; i++)
                {
                    var memberKey = ToMemberKey(elements[i]);
                    if (existing.Add(memberKey))
                    {
                        doc.Members.Add(new HyperLogLogMemberDocument { MemberKey = memberKey, Member = elements[i] });
                        changed = true;
                    }
                }

                if (!changed)
                {
                    return new HyperLogLogAddResult(HyperLogLogResultStatus.Ok, false);
                }

                var replace = await hyperLogLogCollection.ReplaceOneAsync(
                    Builders<HyperLogLogDocument>.Filter.Eq(x => x.Id, doc.Id),
                    doc,
                    new ReplaceOptions(),
                    token);
                if (replace.MatchedCount > 0)
                {
                    return new HyperLogLogAddResult(HyperLogLogResultStatus.Ok, true);
                }
            }

            return new HyperLogLogAddResult(HyperLogLogResultStatus.Ok, false);
        }

        private async Task<HyperLogLogCountResult> HyperLogLogCountCoreAsync(string[] keys, CancellationToken token)
        {
            if (keys.Length == 0)
            {
                return new HyperLogLogCountResult(HyperLogLogResultStatus.Ok, 0);
            }

            var nowUtc = DateTime.UtcNow;
            var allMembers = new HashSet<string>(StringComparer.Ordinal);
            for (int i = 0; i < keys.Length; i++)
            {
                var key = keys[i];
                if (await IsHyperLogLogWrongTypeAsync(key, nowUtc, token))
                {
                    return new HyperLogLogCountResult(HyperLogLogResultStatus.WrongType, 0);
                }

                var doc = await hyperLogLogCollection.Find(ActiveHyperLogLogKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
                if (doc == null)
                {
                    continue;
                }

                for (int memberIndex = 0; memberIndex < doc.Members.Count; memberIndex++)
                {
                    allMembers.Add(doc.Members[memberIndex].MemberKey);
                }
            }

            return new HyperLogLogCountResult(HyperLogLogResultStatus.Ok, allMembers.Count);
        }

        private async Task<HyperLogLogMergeResult> HyperLogLogMergeCoreAsync(string destinationKey, string[] sourceKeys, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsHyperLogLogWrongTypeAsync(destinationKey, nowUtc, token))
            {
                return new HyperLogLogMergeResult(HyperLogLogResultStatus.WrongType);
            }

            var merged = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < sourceKeys.Length; i++)
            {
                var key = sourceKeys[i];
                if (await IsHyperLogLogWrongTypeAsync(key, nowUtc, token))
                {
                    return new HyperLogLogMergeResult(HyperLogLogResultStatus.WrongType);
                }

                var source = await hyperLogLogCollection.Find(ActiveHyperLogLogKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
                if (source == null)
                {
                    continue;
                }

                for (int memberIndex = 0; memberIndex < source.Members.Count; memberIndex++)
                {
                    var member = source.Members[memberIndex];
                    merged[member.MemberKey] = member.Member;
                }
            }

            var destination = await hyperLogLogCollection.Find(ActiveHyperLogLogKeyFilter(destinationKey, nowUtc)).FirstOrDefaultAsync(token);
            if (destination == null)
            {
                var created = new HyperLogLogDocument
                {
                    Key = destinationKey,
                    ExpireAt = null,
                    Members = merged.Select(x => new HyperLogLogMemberDocument { MemberKey = x.Key, Member = x.Value }).ToList()
                };

                try
                {
                    await hyperLogLogCollection.InsertOneAsync(created, null, token);
                }
                catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
                {
                    destination = await hyperLogLogCollection.Find(ActiveHyperLogLogKeyFilter(destinationKey, nowUtc)).FirstOrDefaultAsync(token);
                    if (destination != null)
                    {
                        destination.Members = merged.Select(x => new HyperLogLogMemberDocument { MemberKey = x.Key, Member = x.Value }).ToList();
                        await hyperLogLogCollection.ReplaceOneAsync(Builders<HyperLogLogDocument>.Filter.Eq(x => x.Id, destination.Id), destination, new ReplaceOptions(), token);
                    }
                }

                return new HyperLogLogMergeResult(HyperLogLogResultStatus.Ok);
            }

            destination.Members = merged.Select(x => new HyperLogLogMemberDocument { MemberKey = x.Key, Member = x.Value }).ToList();
            await hyperLogLogCollection.ReplaceOneAsync(Builders<HyperLogLogDocument>.Filter.Eq(x => x.Id, destination.Id), destination, new ReplaceOptions(), token);
            return new HyperLogLogMergeResult(HyperLogLogResultStatus.Ok);
        }

        private async Task<ProbabilisticResultStatus> BloomReserveCoreAsync(string key, double errorRate, long capacity, CancellationToken token)
        {
            if (!TryComputeBloomSizing(errorRate, capacity, out var bitSize, out var hashFunctions))
            {
                return ProbabilisticResultStatus.InvalidArgument;
            }

            var nowUtc = DateTime.UtcNow;
            if (await IsBloomWrongTypeAsync(key, nowUtc, token))
            {
                return ProbabilisticResultStatus.WrongType;
            }

            var existing = await bloomCollection.Find(ActiveBloomKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (existing != null)
            {
                return ProbabilisticResultStatus.Exists;
            }

            var byteSize = checked((int)((bitSize + 7) / 8));
            var created = new BloomDocument
            {
                Key = key,
                ErrorRate = errorRate,
                Capacity = capacity,
                HashFunctions = hashFunctions,
                BitSize = bitSize,
                Bits = new byte[byteSize],
                ItemsInserted = 0,
                ExpireAt = null
            };

            try
            {
                await bloomCollection.InsertOneAsync(created, null, token);
                return ProbabilisticResultStatus.Ok;
            }
            catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
            {
                return ProbabilisticResultStatus.Exists;
            }
        }

        private async Task<ProbabilisticBoolResult> BloomAddCoreAsync(string key, byte[] element, CancellationToken token)
        {
            var result = await BloomMAddCoreAsync(key, new[] { element }, token);
            if (result.Status != ProbabilisticResultStatus.Ok)
            {
                return new ProbabilisticBoolResult(result.Status, false);
            }

            return new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, result.Values.Length > 0 && result.Values[0] != 0);
        }

        private async Task<ProbabilisticArrayResult> BloomMAddCoreAsync(string key, byte[][] elements, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsBloomWrongTypeAsync(key, nowUtc, token))
            {
                return new ProbabilisticArrayResult(ProbabilisticResultStatus.WrongType, Array.Empty<long>());
            }

            if (elements.Length == 0)
            {
                return new ProbabilisticArrayResult(ProbabilisticResultStatus.Ok, Array.Empty<long>());
            }

            for (int attempt = 0; attempt < 5; attempt++)
            {
                token.ThrowIfCancellationRequested();
                nowUtc = DateTime.UtcNow;
                var doc = await bloomCollection.Find(ActiveBloomKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
                if (doc == null)
                {
                    if (!TryComputeBloomSizing(0.01, 1000, out var bitSize, out var hashFunctions))
                    {
                        return new ProbabilisticArrayResult(ProbabilisticResultStatus.InvalidArgument, Array.Empty<long>());
                    }

                    doc = new BloomDocument
                    {
                        Key = key,
                        ErrorRate = 0.01,
                        Capacity = 1000,
                        HashFunctions = hashFunctions,
                        BitSize = bitSize,
                        Bits = new byte[checked((int)((bitSize + 7) / 8))],
                        ItemsInserted = 0,
                        ExpireAt = null
                    };

                    try
                    {
                        await bloomCollection.InsertOneAsync(doc, null, token);
                    }
                    catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
                    {
                        continue;
                    }
                }

                var values = new long[elements.Length];
                var changedAny = false;
                for (int i = 0; i < elements.Length; i++)
                {
                    var changed = SetBloomBitsAndReturnChanged(doc, elements[i]);
                    values[i] = changed ? 1 : 0;
                    if (changed)
                    {
                        changedAny = true;
                        doc.ItemsInserted++;
                    }
                }

                if (changedAny)
                {
                    var replaced = await bloomCollection.ReplaceOneAsync(
                        Builders<BloomDocument>.Filter.Eq(x => x.Id, doc.Id),
                        doc,
                        new ReplaceOptions(),
                        token);
                    if (replaced.MatchedCount == 0)
                    {
                        continue;
                    }
                }

                return new ProbabilisticArrayResult(ProbabilisticResultStatus.Ok, values);
            }

            return new ProbabilisticArrayResult(ProbabilisticResultStatus.Ok, Array.Empty<long>());
        }

        private async Task<ProbabilisticBoolResult> BloomExistsCoreAsync(string key, byte[] element, CancellationToken token)
        {
            var result = await BloomMExistsCoreAsync(key, new[] { element }, token);
            if (result.Status != ProbabilisticResultStatus.Ok)
            {
                return new ProbabilisticBoolResult(result.Status, false);
            }

            return new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, result.Values.Length > 0 && result.Values[0] != 0);
        }

        private async Task<ProbabilisticArrayResult> BloomMExistsCoreAsync(string key, byte[][] elements, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsBloomWrongTypeAsync(key, nowUtc, token))
            {
                return new ProbabilisticArrayResult(ProbabilisticResultStatus.WrongType, Array.Empty<long>());
            }

            if (elements.Length == 0)
            {
                return new ProbabilisticArrayResult(ProbabilisticResultStatus.Ok, Array.Empty<long>());
            }

            var doc = await bloomCollection.Find(ActiveBloomKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return new ProbabilisticArrayResult(ProbabilisticResultStatus.Ok, new long[elements.Length]);
            }

            var values = new long[elements.Length];
            for (int i = 0; i < elements.Length; i++)
            {
                values[i] = BloomMayContain(doc, elements[i]) ? 1 : 0;
            }

            return new ProbabilisticArrayResult(ProbabilisticResultStatus.Ok, values);
        }

        private async Task<ProbabilisticInfoResult> BloomInfoCoreAsync(string key, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsBloomWrongTypeAsync(key, nowUtc, token))
            {
                return new ProbabilisticInfoResult(ProbabilisticResultStatus.WrongType, Array.Empty<KeyValuePair<string, string>>());
            }

            var doc = await bloomCollection.Find(ActiveBloomKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return new ProbabilisticInfoResult(ProbabilisticResultStatus.NotFound, Array.Empty<KeyValuePair<string, string>>());
            }

            var values = new[]
            {
                new KeyValuePair<string, string>("Capacity", doc.Capacity.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("Size", doc.BitSize.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("Number of filters", "1"),
                new KeyValuePair<string, string>("Number of items inserted", doc.ItemsInserted.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("Expansion rate", "2")
            };

            return new ProbabilisticInfoResult(ProbabilisticResultStatus.Ok, values);
        }

        private async Task<ProbabilisticResultStatus> CuckooReserveCoreAsync(string key, long capacity, CancellationToken token)
        {
            if (capacity <= 0)
            {
                return ProbabilisticResultStatus.InvalidArgument;
            }

            var nowUtc = DateTime.UtcNow;
            if (await IsCuckooWrongTypeAsync(key, nowUtc, token))
            {
                return ProbabilisticResultStatus.WrongType;
            }

            var existing = await cuckooCollection.Find(ActiveCuckooKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (existing != null)
            {
                return ProbabilisticResultStatus.Exists;
            }

            var created = new CuckooDocument { Key = key, Capacity = capacity, ExpireAt = null, Items = new List<CuckooItemDocument>() };
            try
            {
                await cuckooCollection.InsertOneAsync(created, null, token);
                return ProbabilisticResultStatus.Ok;
            }
            catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
            {
                return ProbabilisticResultStatus.Exists;
            }
        }

        private async Task<ProbabilisticBoolResult> CuckooAddCoreAsync(string key, byte[] item, bool noCreate, bool nx, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsCuckooWrongTypeAsync(key, nowUtc, token))
            {
                return new ProbabilisticBoolResult(ProbabilisticResultStatus.WrongType, false);
            }

            for (int attempt = 0; attempt < 5; attempt++)
            {
                token.ThrowIfCancellationRequested();
                nowUtc = DateTime.UtcNow;
                var doc = await cuckooCollection.Find(ActiveCuckooKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
                if (doc == null)
                {
                    if (noCreate)
                    {
                        return new ProbabilisticBoolResult(ProbabilisticResultStatus.NotFound, false);
                    }

                    try
                    {
                        doc = new CuckooDocument { Key = key, Capacity = 1024, ExpireAt = null, Items = new List<CuckooItemDocument>() };
                        await cuckooCollection.InsertOneAsync(doc, null, token);
                    }
                    catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
                    {
                        continue;
                    }
                }

                var itemKey = ToMemberKey(item);
                var existing = doc.Items.FirstOrDefault(x => string.Equals(x.ItemKey, itemKey, StringComparison.Ordinal));
                if (existing == null)
                {
                    doc.Items.Add(new CuckooItemDocument { ItemKey = itemKey, Item = item, Count = 1 });
                }
                else
                {
                    if (nx)
                    {
                        return new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, false);
                    }

                    existing.Count++;
                }

                var replaced = await cuckooCollection.ReplaceOneAsync(
                    Builders<CuckooDocument>.Filter.Eq(x => x.Id, doc.Id),
                    doc,
                    new ReplaceOptions(),
                    token);
                if (replaced.MatchedCount > 0)
                {
                    return new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, true);
                }
            }

            return new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, false);
        }

        private async Task<ProbabilisticBoolResult> CuckooExistsCoreAsync(string key, byte[] item, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsCuckooWrongTypeAsync(key, nowUtc, token))
            {
                return new ProbabilisticBoolResult(ProbabilisticResultStatus.WrongType, false);
            }

            var doc = await cuckooCollection.Find(ActiveCuckooKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, false);
            }

            var itemKey = ToMemberKey(item);
            var exists = doc.Items.Any(x => string.Equals(x.ItemKey, itemKey, StringComparison.Ordinal) && x.Count > 0);
            return new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, exists);
        }

        private async Task<ProbabilisticBoolResult> CuckooDeleteCoreAsync(string key, byte[] item, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsCuckooWrongTypeAsync(key, nowUtc, token))
            {
                return new ProbabilisticBoolResult(ProbabilisticResultStatus.WrongType, false);
            }

            var doc = await cuckooCollection.Find(ActiveCuckooKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, false);
            }

            var itemKey = ToMemberKey(item);
            var existing = doc.Items.FirstOrDefault(x => string.Equals(x.ItemKey, itemKey, StringComparison.Ordinal));
            if (existing == null || existing.Count <= 0)
            {
                return new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, false);
            }

            existing.Count--;
            if (existing.Count <= 0)
            {
                doc.Items.RemoveAll(x => string.Equals(x.ItemKey, itemKey, StringComparison.Ordinal));
            }

            if (doc.Items.Count == 0)
            {
                await cuckooCollection.DeleteOneAsync(Builders<CuckooDocument>.Filter.Eq(x => x.Id, doc.Id), token);
            }
            else
            {
                await cuckooCollection.ReplaceOneAsync(
                    Builders<CuckooDocument>.Filter.Eq(x => x.Id, doc.Id),
                    doc,
                    new ReplaceOptions(),
                    token);
            }

            return new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, true);
        }

        private async Task<ProbabilisticCountResult> CuckooCountCoreAsync(string key, byte[] item, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsCuckooWrongTypeAsync(key, nowUtc, token))
            {
                return new ProbabilisticCountResult(ProbabilisticResultStatus.WrongType, 0);
            }

            var doc = await cuckooCollection.Find(ActiveCuckooKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return new ProbabilisticCountResult(ProbabilisticResultStatus.Ok, 0);
            }

            var itemKey = ToMemberKey(item);
            var count = doc.Items.FirstOrDefault(x => string.Equals(x.ItemKey, itemKey, StringComparison.Ordinal))?.Count ?? 0;
            return new ProbabilisticCountResult(ProbabilisticResultStatus.Ok, count);
        }

        private async Task<ProbabilisticInfoResult> CuckooInfoCoreAsync(string key, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsCuckooWrongTypeAsync(key, nowUtc, token))
            {
                return new ProbabilisticInfoResult(ProbabilisticResultStatus.WrongType, Array.Empty<KeyValuePair<string, string>>());
            }

            var doc = await cuckooCollection.Find(ActiveCuckooKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return new ProbabilisticInfoResult(ProbabilisticResultStatus.NotFound, Array.Empty<KeyValuePair<string, string>>());
            }

            var inserted = doc.Items.Sum(x => x.Count);
            var values = new[]
            {
                new KeyValuePair<string, string>("Size", doc.Items.Count.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("Number of buckets", doc.Capacity.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("Number of items inserted", inserted.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("Bucket size", "2"),
                new KeyValuePair<string, string>("Max iterations", "20"),
                new KeyValuePair<string, string>("Expansion rate", "1")
            };

            return new ProbabilisticInfoResult(ProbabilisticResultStatus.Ok, values);
        }

        private async Task<ProbabilisticResultStatus> TDigestCreateCoreAsync(string key, int compression, CancellationToken token)
        {
            if (compression <= 0)
            {
                return ProbabilisticResultStatus.InvalidArgument;
            }

            var nowUtc = DateTime.UtcNow;
            if (await IsTDigestWrongTypeAsync(key, nowUtc, token))
            {
                return ProbabilisticResultStatus.WrongType;
            }

            var existing = await tdigestCollection.Find(ActiveTDigestKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (existing != null)
            {
                return ProbabilisticResultStatus.Exists;
            }

            var created = new TDigestDocument { Key = key, Compression = compression, Values = new List<double>(), ExpireAt = null };
            try
            {
                await tdigestCollection.InsertOneAsync(created, null, token);
                return ProbabilisticResultStatus.Ok;
            }
            catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
            {
                return ProbabilisticResultStatus.Exists;
            }
        }

        private async Task<ProbabilisticResultStatus> TDigestResetCoreAsync(string key, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsTDigestWrongTypeAsync(key, nowUtc, token))
            {
                return ProbabilisticResultStatus.WrongType;
            }

            var update = Builders<TDigestDocument>.Update.Set(x => x.Values, new List<double>());
            var result = await tdigestCollection.UpdateOneAsync(ActiveTDigestKeyFilter(key, nowUtc), update, null, token);
            if (result.MatchedCount == 0)
            {
                return ProbabilisticResultStatus.NotFound;
            }

            return ProbabilisticResultStatus.Ok;
        }

        private async Task<ProbabilisticResultStatus> TDigestAddCoreAsync(string key, double[] values, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsTDigestWrongTypeAsync(key, nowUtc, token))
            {
                return ProbabilisticResultStatus.WrongType;
            }

            for (int attempt = 0; attempt < 5; attempt++)
            {
                token.ThrowIfCancellationRequested();
                nowUtc = DateTime.UtcNow;
                var doc = await tdigestCollection.Find(ActiveTDigestKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
                if (doc == null)
                {
                    return ProbabilisticResultStatus.NotFound;
                }

                if (values.Length > 0)
                {
                    doc.Values.AddRange(values);
                    doc.Values.Sort();
                    var replaced = await tdigestCollection.ReplaceOneAsync(
                        Builders<TDigestDocument>.Filter.Eq(x => x.Id, doc.Id),
                        doc,
                        new ReplaceOptions(),
                        token);
                    if (replaced.MatchedCount == 0)
                    {
                        continue;
                    }
                }

                return ProbabilisticResultStatus.Ok;
            }

            return ProbabilisticResultStatus.NotFound;
        }

        private async Task<ProbabilisticDoubleArrayResult> TDigestQuantileCoreAsync(string key, double[] quantiles, CancellationToken token)
        {
            for (int i = 0; i < quantiles.Length; i++)
            {
                if (quantiles[i] < 0 || quantiles[i] > 1)
                {
                    return new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.InvalidArgument, Array.Empty<double>());
                }
            }

            var nowUtc = DateTime.UtcNow;
            if (await IsTDigestWrongTypeAsync(key, nowUtc, token))
            {
                return new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.WrongType, Array.Empty<double>());
            }

            var doc = await tdigestCollection.Find(ActiveTDigestKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.NotFound, Array.Empty<double>());
            }

            var output = new double[quantiles.Length];
            for (int i = 0; i < quantiles.Length; i++)
            {
                output[i] = ComputeQuantile(doc.Values, quantiles[i]);
            }

            return new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.Ok, output);
        }

        private async Task<ProbabilisticDoubleArrayResult> TDigestCdfCoreAsync(string key, double[] values, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsTDigestWrongTypeAsync(key, nowUtc, token))
            {
                return new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.WrongType, Array.Empty<double>());
            }

            var doc = await tdigestCollection.Find(ActiveTDigestKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.NotFound, Array.Empty<double>());
            }

            var output = new double[values.Length];
            for (int i = 0; i < values.Length; i++)
            {
                output[i] = ComputeCdf(doc.Values, values[i]);
            }

            return new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.Ok, output);
        }

        private async Task<ProbabilisticArrayResult> TDigestRankCoreAsync(string key, double[] values, bool reverse, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsTDigestWrongTypeAsync(key, nowUtc, token))
            {
                return new ProbabilisticArrayResult(ProbabilisticResultStatus.WrongType, Array.Empty<long>());
            }

            var doc = await tdigestCollection.Find(ActiveTDigestKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return new ProbabilisticArrayResult(ProbabilisticResultStatus.NotFound, Array.Empty<long>());
            }

            var output = new long[values.Length];
            for (int i = 0; i < values.Length; i++)
            {
                output[i] = reverse
                    ? ComputeReverseRank(doc.Values, values[i])
                    : ComputeRank(doc.Values, values[i]);
            }

            return new ProbabilisticArrayResult(ProbabilisticResultStatus.Ok, output);
        }

        private async Task<ProbabilisticDoubleArrayResult> TDigestByRankCoreAsync(string key, long[] ranks, bool reverse, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsTDigestWrongTypeAsync(key, nowUtc, token))
            {
                return new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.WrongType, Array.Empty<double>());
            }

            var doc = await tdigestCollection.Find(ActiveTDigestKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.NotFound, Array.Empty<double>());
            }

            var output = new double[ranks.Length];
            for (int i = 0; i < ranks.Length; i++)
            {
                output[i] = ComputeByRank(doc.Values, ranks[i], reverse);
            }

            return new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.Ok, output);
        }

        private async Task<ProbabilisticDoubleResult> TDigestTrimmedMeanCoreAsync(string key, double lowerQuantile, double upperQuantile, CancellationToken token)
        {
            if (lowerQuantile < 0 || lowerQuantile > 1 || upperQuantile < 0 || upperQuantile > 1 || lowerQuantile > upperQuantile)
            {
                return new ProbabilisticDoubleResult(ProbabilisticResultStatus.InvalidArgument, null);
            }

            var nowUtc = DateTime.UtcNow;
            if (await IsTDigestWrongTypeAsync(key, nowUtc, token))
            {
                return new ProbabilisticDoubleResult(ProbabilisticResultStatus.WrongType, null);
            }

            var doc = await tdigestCollection.Find(ActiveTDigestKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return new ProbabilisticDoubleResult(ProbabilisticResultStatus.NotFound, null);
            }

            var mean = ComputeTrimmedMean(doc.Values, lowerQuantile, upperQuantile);
            return new ProbabilisticDoubleResult(ProbabilisticResultStatus.Ok, mean);
        }

        private async Task<ProbabilisticDoubleResult> TDigestMinMaxCoreAsync(string key, bool max, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsTDigestWrongTypeAsync(key, nowUtc, token))
            {
                return new ProbabilisticDoubleResult(ProbabilisticResultStatus.WrongType, null);
            }

            var doc = await tdigestCollection.Find(ActiveTDigestKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null || doc.Values.Count == 0)
            {
                return new ProbabilisticDoubleResult(ProbabilisticResultStatus.NotFound, null);
            }

            var value = max ? doc.Values[^1] : doc.Values[0];
            return new ProbabilisticDoubleResult(ProbabilisticResultStatus.Ok, value);
        }

        private async Task<ProbabilisticInfoResult> TDigestInfoCoreAsync(string key, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsTDigestWrongTypeAsync(key, nowUtc, token))
            {
                return new ProbabilisticInfoResult(ProbabilisticResultStatus.WrongType, Array.Empty<KeyValuePair<string, string>>());
            }

            var doc = await tdigestCollection.Find(ActiveTDigestKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return new ProbabilisticInfoResult(ProbabilisticResultStatus.NotFound, Array.Empty<KeyValuePair<string, string>>());
            }

            var values = new[]
            {
                new KeyValuePair<string, string>("Compression", doc.Compression.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("Observations", doc.Values.Count.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("Min", doc.Values.Count == 0 ? "NaN" : doc.Values[0].ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("Max", doc.Values.Count == 0 ? "NaN" : doc.Values[^1].ToString(CultureInfo.InvariantCulture))
            };

            return new ProbabilisticInfoResult(ProbabilisticResultStatus.Ok, values);
        }

        private static double ComputeQuantile(List<double> values, double quantile)
        {
            if (values.Count == 0)
            {
                return double.NaN;
            }

            if (quantile <= 0)
            {
                return values[0];
            }

            if (quantile >= 1)
            {
                return values[^1];
            }

            var position = quantile * (values.Count - 1);
            var lower = (int)Math.Floor(position);
            var upper = (int)Math.Ceiling(position);
            if (lower == upper)
            {
                return values[lower];
            }

            var fraction = position - lower;
            return values[lower] + ((values[upper] - values[lower]) * fraction);
        }

        private static double ComputeCdf(List<double> values, double value)
        {
            if (values.Count == 0)
            {
                return double.NaN;
            }

            var upperBound = values.BinarySearch(value);
            if (upperBound < 0)
            {
                upperBound = ~upperBound;
            }
            else
            {
                while (upperBound < values.Count && values[upperBound] <= value)
                {
                    upperBound++;
                }
            }

            return upperBound / (double)values.Count;
        }

        private static long ComputeRank(List<double> values, double value)
        {
            if (values.Count == 0)
            {
                return -1;
            }

            var upperBound = values.BinarySearch(value);
            if (upperBound < 0)
            {
                upperBound = ~upperBound;
            }
            else
            {
                while (upperBound < values.Count && values[upperBound] <= value)
                {
                    upperBound++;
                }
            }

            return upperBound - 1;
        }

        private static long ComputeReverseRank(List<double> values, double value)
        {
            if (values.Count == 0)
            {
                return -1;
            }

            var lowerBound = values.BinarySearch(value);
            if (lowerBound < 0)
            {
                lowerBound = ~lowerBound;
            }
            else
            {
                while (lowerBound > 0 && values[lowerBound - 1] >= value)
                {
                    lowerBound--;
                }
            }

            return values.Count - lowerBound - 1;
        }

        private static double ComputeByRank(List<double> values, long rank, bool reverse)
        {
            if (rank < 0 || rank >= values.Count)
            {
                return double.NaN;
            }

            var index = reverse ? (values.Count - 1 - (int)rank) : (int)rank;
            return values[index];
        }

        private static double ComputeTrimmedMean(List<double> values, double lowerQuantile, double upperQuantile)
        {
            if (values.Count == 0)
            {
                return double.NaN;
            }

            var start = (int)Math.Ceiling(lowerQuantile * (values.Count - 1));
            var end = (int)Math.Floor(upperQuantile * (values.Count - 1));
            if (end < start)
            {
                return double.NaN;
            }

            var count = end - start + 1;
            var sum = 0.0;
            for (int i = start; i <= end; i++)
            {
                sum += values[i];
            }

            return sum / count;
        }

        private async Task<string?> StreamAddCoreAsync(string key, string id, KeyValuePair<string, byte[]>[] fields, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsStreamWrongTypeAsync(key, nowUtc, token))
            {
                return null;
            }

            for (int attempt = 0; attempt < 5; attempt++)
            {
                token.ThrowIfCancellationRequested();
                nowUtc = DateTime.UtcNow;
                var doc = await streamCollection.Find(ActiveStreamKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);

                long lastMs = -1;
                long lastSeq = -1;
                if (doc != null && !string.IsNullOrEmpty(doc.LastId) && TryParseStreamId(doc.LastId!, out var parsedLastMs, out var parsedLastSeq))
                {
                    lastMs = parsedLastMs;
                    lastSeq = parsedLastSeq;
                }

                long newMs;
                long newSeq;

                if (id == "*")
                {
                    newMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    if (newMs < lastMs)
                    {
                        newMs = lastMs;
                    }

                    newSeq = newMs == lastMs ? lastSeq + 1 : 0;
                }
                else if (id.EndsWith("-*", StringComparison.Ordinal))
                {
                    var msText = id.Substring(0, id.Length - 2);
                    if (!long.TryParse(msText, NumberStyles.Integer, CultureInfo.InvariantCulture, out newMs) || newMs < 0)
                    {
                        return null;
                    }

                    if (newMs < lastMs)
                    {
                        return null;
                    }

                    newSeq = newMs == lastMs ? lastSeq + 1 : 0;
                }
                else
                {
                    if (!TryParseStreamId(id, out newMs, out newSeq))
                    {
                        return null;
                    }

                    if (CompareStreamIds(newMs, newSeq, lastMs, lastSeq) <= 0)
                    {
                        return null;
                    }
                }

                var newId = $"{newMs.ToString(CultureInfo.InvariantCulture)}-{newSeq.ToString(CultureInfo.InvariantCulture)}";
                var newEntry = new StreamEntryDocument
                {
                    Id = newId,
                    Milliseconds = newMs,
                    Sequence = newSeq,
                    Fields = fields.Select(x => new StreamFieldDocument { Field = x.Key, Value = x.Value }).ToList()
                };

                if (doc == null)
                {
                    try
                    {
                        var created = new StreamDocument
                        {
                            Key = key,
                            LastId = newId,
                            ExpireAt = null,
                            Entries = new List<StreamEntryDocument> { newEntry }
                        };

                        await streamCollection.InsertOneAsync(created, null, token);
                        return newId;
                    }
                    catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
                    {
                        continue;
                    }
                }

                doc.Entries.Add(newEntry);
                doc.LastId = newId;
                doc.Entries = doc.Entries
                    .OrderBy(x => x.Milliseconds)
                    .ThenBy(x => x.Sequence)
                    .ToList();

                var replace = await streamCollection.ReplaceOneAsync(
                    Builders<StreamDocument>.Filter.Eq(x => x.Id, doc.Id),
                    doc,
                    new ReplaceOptions(),
                    token);
                if (replace.MatchedCount > 0)
                {
                    return newId;
                }
            }

            return null;
        }

        private async Task<long> StreamDeleteCoreAsync(string key, string[] ids, CancellationToken token)
        {
            if (ids.Length == 0)
            {
                return 0;
            }

            var nowUtc = DateTime.UtcNow;
            if (await IsStreamWrongTypeAsync(key, nowUtc, token))
            {
                return 0;
            }

            var doc = await streamCollection.Find(ActiveStreamKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null || doc.Entries.Count == 0)
            {
                return 0;
            }

            var idSet = new HashSet<string>(ids, StringComparer.Ordinal);
            var original = doc.Entries.Count;
            doc.Entries.RemoveAll(x => idSet.Contains(x.Id));
            var removed = original - doc.Entries.Count;
            if (removed <= 0)
            {
                return 0;
            }

            if (doc.Entries.Count == 0)
            {
                await streamCollection.DeleteOneAsync(Builders<StreamDocument>.Filter.Eq(x => x.Id, doc.Id), token);
            }
            else
            {
                var last = doc.Entries[^1];
                doc.LastId = last.Id;
                RemovePendingForMissingEntries(doc);
                await streamCollection.ReplaceOneAsync(Builders<StreamDocument>.Filter.Eq(x => x.Id, doc.Id), doc, new ReplaceOptions(), token);
            }

            return removed;
        }

        private async Task<long> StreamLengthCoreAsync(string key, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsStreamWrongTypeAsync(key, nowUtc, token))
            {
                return 0;
            }

            var doc = await streamCollection.Find(ActiveStreamKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            return doc?.Entries.Count ?? 0;
        }

        private async Task<string?> StreamLastIdCoreAsync(string key, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsStreamWrongTypeAsync(key, nowUtc, token))
            {
                return null;
            }

            var doc = await streamCollection.Find(ActiveStreamKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            return doc?.LastId;
        }

        private async Task<StreamEntry[]> StreamRangeCoreAsync(string key, string start, string end, int? count, bool reverse, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsStreamWrongTypeAsync(key, nowUtc, token))
            {
                return Array.Empty<StreamEntry>();
            }

            var doc = await streamCollection.Find(ActiveStreamKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null || doc.Entries.Count == 0)
            {
                return Array.Empty<StreamEntry>();
            }

            long startMs;
            long startSeq;
            if (start == "-")
            {
                startMs = long.MinValue;
                startSeq = long.MinValue;
            }
            else if (start == "+")
            {
                startMs = long.MaxValue;
                startSeq = long.MaxValue;
            }
            else if (!TryParseStreamId(start, out startMs, out startSeq))
            {
                return Array.Empty<StreamEntry>();
            }

            long endMs;
            long endSeq;
            if (end == "+")
            {
                endMs = long.MaxValue;
                endSeq = long.MaxValue;
            }
            else if (end == "-")
            {
                endMs = long.MinValue;
                endSeq = long.MinValue;
            }
            else if (!TryParseStreamId(end, out endMs, out endSeq))
            {
                return Array.Empty<StreamEntry>();
            }

            IEnumerable<StreamEntryDocument> query = doc.Entries.Where(x =>
                CompareStreamIds(x.Milliseconds, x.Sequence, startMs, startSeq) >= 0 &&
                CompareStreamIds(x.Milliseconds, x.Sequence, endMs, endSeq) <= 0);

            query = reverse
                ? query.OrderByDescending(x => x.Milliseconds).ThenByDescending(x => x.Sequence)
                : query.OrderBy(x => x.Milliseconds).ThenBy(x => x.Sequence);

            if (count.HasValue && count.Value > 0)
            {
                query = query.Take(count.Value);
            }

            return query.Select(ToStreamEntry).ToArray();
        }

        private async Task<StreamReadResult[]> StreamReadCoreAsync(string[] keys, string[] ids, int? count, CancellationToken token)
        {
            if (keys.Length == 0 || ids.Length == 0 || keys.Length != ids.Length)
            {
                return Array.Empty<StreamReadResult>();
            }

            var nowUtc = DateTime.UtcNow;
            var results = new List<StreamReadResult>(keys.Length);
            for (int i = 0; i < keys.Length; i++)
            {
                var key = keys[i];
                if (await IsStreamWrongTypeAsync(key, nowUtc, token))
                {
                    continue;
                }

                var doc = await streamCollection.Find(ActiveStreamKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
                if (doc == null || doc.Entries.Count == 0)
                {
                    continue;
                }

                if (!TryParseStreamId(ids[i], out var fromMs, out var fromSeq))
                {
                    continue;
                }

                IEnumerable<StreamEntryDocument> query = doc.Entries
                    .Where(x => CompareStreamIds(x.Milliseconds, x.Sequence, fromMs, fromSeq) > 0)
                    .OrderBy(x => x.Milliseconds)
                    .ThenBy(x => x.Sequence);

                if (count.HasValue && count.Value > 0)
                {
                    query = query.Take(count.Value);
                }

                var entries = query.Select(ToStreamEntry).ToArray();
                if (entries.Length > 0)
                {
                    results.Add(new StreamReadResult(key, entries));
                }
            }

            return results.ToArray();
        }

        private async Task<StreamSetIdResultStatus> StreamSetIdCoreAsync(string key, string lastId, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsStreamWrongTypeAsync(key, nowUtc, token))
            {
                return StreamSetIdResultStatus.WrongType;
            }

            if (!TryParseStreamId(lastId, out var newMs, out var newSeq))
            {
                return StreamSetIdResultStatus.InvalidId;
            }

            var doc = await streamCollection.Find(ActiveStreamKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                try
                {
                    var created = new StreamDocument { Key = key, LastId = lastId, ExpireAt = null };
                    await streamCollection.InsertOneAsync(created, null, token);
                    return StreamSetIdResultStatus.Ok;
                }
                catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
                {
                    return StreamSetIdResultStatus.Ok;
                }
            }

            if (doc.Entries.Count > 0)
            {
                var maxEntry = doc.Entries
                    .OrderByDescending(x => x.Milliseconds)
                    .ThenByDescending(x => x.Sequence)
                    .First();
                if (CompareStreamIds(newMs, newSeq, maxEntry.Milliseconds, maxEntry.Sequence) < 0)
                {
                    return StreamSetIdResultStatus.InvalidId;
                }
            }

            doc.LastId = lastId;
            await streamCollection.ReplaceOneAsync(Builders<StreamDocument>.Filter.Eq(x => x.Id, doc.Id), doc, new ReplaceOptions(), token);
            return StreamSetIdResultStatus.Ok;
        }

        private async Task<long> StreamTrimCoreAsync(string key, int? maxLength, string? minId, bool approximate, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsStreamWrongTypeAsync(key, nowUtc, token))
            {
                return 0;
            }

            var doc = await streamCollection.Find(ActiveStreamKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null || doc.Entries.Count == 0)
            {
                return 0;
            }

            var original = doc.Entries.Count;

            if (maxLength.HasValue && maxLength.Value >= 0 && doc.Entries.Count > maxLength.Value)
            {
                var toRemove = doc.Entries.Count - maxLength.Value;
                doc.Entries.RemoveRange(0, toRemove);
            }

            if (!string.IsNullOrEmpty(minId) && TryParseStreamId(minId!, out var minMs, out var minSeq))
            {
                doc.Entries = doc.Entries
                    .Where(x => CompareStreamIds(x.Milliseconds, x.Sequence, minMs, minSeq) >= 0)
                    .ToList();
            }

            var removed = original - doc.Entries.Count;
            if (removed <= 0)
            {
                return 0;
            }

            if (doc.Entries.Count == 0)
            {
                await streamCollection.DeleteOneAsync(Builders<StreamDocument>.Filter.Eq(x => x.Id, doc.Id), token);
                return removed;
            }

            var last = doc.Entries[^1];
            doc.LastId = last.Id;
            RemovePendingForMissingEntries(doc);
            await streamCollection.ReplaceOneAsync(Builders<StreamDocument>.Filter.Eq(x => x.Id, doc.Id), doc, new ReplaceOptions(), token);
            return removed;
        }

        private async Task<StreamInfoResult> StreamInfoCoreAsync(string key, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsStreamWrongTypeAsync(key, nowUtc, token))
            {
                return new StreamInfoResult(StreamInfoResultStatus.WrongType, null);
            }

            var doc = await streamCollection.Find(ActiveStreamKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return new StreamInfoResult(StreamInfoResultStatus.NoStream, null);
            }

            StreamEntry? firstEntry = null;
            StreamEntry? lastEntry = null;
            if (doc.Entries.Count > 0)
            {
                var ordered = doc.Entries
                    .OrderBy(x => x.Milliseconds)
                    .ThenBy(x => x.Sequence)
                    .ToList();
                firstEntry = ToStreamEntry(ordered[0]);
                lastEntry = ToStreamEntry(ordered[^1]);
            }

            var info = new StreamInfo(doc.Entries.Count, doc.LastId, firstEntry, lastEntry);
            return new StreamInfoResult(StreamInfoResultStatus.Ok, info);
        }

        private async Task<StreamGroupCreateResult> StreamGroupCreateCoreAsync(string key, string group, string startId, bool mkStream, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsStreamWrongTypeAsync(key, nowUtc, token))
            {
                return StreamGroupCreateResult.WrongType;
            }

            for (int attempt = 0; attempt < 5; attempt++)
            {
                token.ThrowIfCancellationRequested();
                nowUtc = DateTime.UtcNow;
                var doc = await streamCollection.Find(ActiveStreamKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
                if (!IsValidGroupStartId(doc, startId))
                {
                    return StreamGroupCreateResult.InvalidId;
                }

                if (doc == null)
                {
                    if (!mkStream)
                    {
                        return StreamGroupCreateResult.NoStream;
                    }

                    var created = new StreamDocument
                    {
                        Key = key,
                        Entries = new List<StreamEntryDocument>(),
                        LastId = null,
                        ExpireAt = null,
                        Groups = new List<StreamGroupDocument>
                        {
                            new StreamGroupDocument
                            {
                                Name = group,
                                LastDeliveredId = ResolveGroupStartId(null, startId),
                                Consumers = new List<StreamConsumerDocument>(),
                                Pending = new List<StreamPendingDocument>()
                            }
                        }
                    };

                    try
                    {
                        await streamCollection.InsertOneAsync(created, null, token);
                        return StreamGroupCreateResult.Ok;
                    }
                    catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
                    {
                        continue;
                    }
                }

                if (doc.Groups.Any(x => string.Equals(x.Name, group, StringComparison.Ordinal)))
                {
                    return StreamGroupCreateResult.Exists;
                }

                doc.Groups.Add(new StreamGroupDocument
                {
                    Name = group,
                    LastDeliveredId = ResolveGroupStartId(doc, startId),
                    Consumers = new List<StreamConsumerDocument>(),
                    Pending = new List<StreamPendingDocument>()
                });

                var replace = await streamCollection.ReplaceOneAsync(
                    Builders<StreamDocument>.Filter.Eq(x => x.Id, doc.Id),
                    doc,
                    new ReplaceOptions(),
                    token);
                if (replace.MatchedCount > 0)
                {
                    return StreamGroupCreateResult.Ok;
                }
            }

            return StreamGroupCreateResult.Exists;
        }

        private async Task<StreamGroupDestroyResult> StreamGroupDestroyCoreAsync(string key, string group, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsStreamWrongTypeAsync(key, nowUtc, token))
            {
                return StreamGroupDestroyResult.WrongType;
            }

            var doc = await streamCollection.Find(ActiveStreamKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return StreamGroupDestroyResult.NotFound;
            }

            var removed = doc.Groups.RemoveAll(x => string.Equals(x.Name, group, StringComparison.Ordinal));
            if (removed <= 0)
            {
                return StreamGroupDestroyResult.NotFound;
            }

            await streamCollection.ReplaceOneAsync(Builders<StreamDocument>.Filter.Eq(x => x.Id, doc.Id), doc, new ReplaceOptions(), token);
            return StreamGroupDestroyResult.Removed;
        }

        private async Task<StreamGroupSetIdResultStatus> StreamGroupSetIdCoreAsync(string key, string group, string lastId, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsStreamWrongTypeAsync(key, nowUtc, token))
            {
                return StreamGroupSetIdResultStatus.WrongType;
            }

            if (!TryParseStreamId(lastId, out _, out _))
            {
                return StreamGroupSetIdResultStatus.InvalidId;
            }

            var doc = await streamCollection.Find(ActiveStreamKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return StreamGroupSetIdResultStatus.NoStream;
            }

            var groupDoc = doc.Groups.FirstOrDefault(x => string.Equals(x.Name, group, StringComparison.Ordinal));
            if (groupDoc == null)
            {
                return StreamGroupSetIdResultStatus.NoGroup;
            }

            groupDoc.LastDeliveredId = lastId;
            await streamCollection.ReplaceOneAsync(Builders<StreamDocument>.Filter.Eq(x => x.Id, doc.Id), doc, new ReplaceOptions(), token);
            return StreamGroupSetIdResultStatus.Ok;
        }

        private async Task<StreamGroupDelConsumerResult> StreamGroupDelConsumerCoreAsync(string key, string group, string consumer, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsStreamWrongTypeAsync(key, nowUtc, token))
            {
                return new StreamGroupDelConsumerResult(StreamGroupDelConsumerResultStatus.WrongType, 0);
            }

            var doc = await streamCollection.Find(ActiveStreamKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return new StreamGroupDelConsumerResult(StreamGroupDelConsumerResultStatus.NoStream, 0);
            }

            var groupDoc = doc.Groups.FirstOrDefault(x => string.Equals(x.Name, group, StringComparison.Ordinal));
            if (groupDoc == null)
            {
                return new StreamGroupDelConsumerResult(StreamGroupDelConsumerResultStatus.NoGroup, 0);
            }

            var removed = groupDoc.Pending.RemoveAll(x => string.Equals(x.Consumer, consumer, StringComparison.Ordinal));
            groupDoc.Consumers.RemoveAll(x => string.Equals(x.Name, consumer, StringComparison.Ordinal));

            await streamCollection.ReplaceOneAsync(Builders<StreamDocument>.Filter.Eq(x => x.Id, doc.Id), doc, new ReplaceOptions(), token);
            return new StreamGroupDelConsumerResult(StreamGroupDelConsumerResultStatus.Ok, removed);
        }

        private async Task<StreamGroupsInfoResult> StreamGroupsInfoCoreAsync(string key, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsStreamWrongTypeAsync(key, nowUtc, token))
            {
                return new StreamGroupsInfoResult(StreamInfoResultStatus.WrongType, Array.Empty<StreamGroupInfo>());
            }

            var doc = await streamCollection.Find(ActiveStreamKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return new StreamGroupsInfoResult(StreamInfoResultStatus.NoStream, Array.Empty<StreamGroupInfo>());
            }

            var groups = doc.Groups
                .Select(x => new StreamGroupInfo(x.Name, x.Consumers.Count, x.Pending.Count, x.LastDeliveredId))
                .ToArray();
            return new StreamGroupsInfoResult(StreamInfoResultStatus.Ok, groups);
        }

        private async Task<StreamConsumersInfoResult> StreamConsumersInfoCoreAsync(string key, string group, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsStreamWrongTypeAsync(key, nowUtc, token))
            {
                return new StreamConsumersInfoResult(StreamInfoResultStatus.WrongType, Array.Empty<StreamConsumerInfo>());
            }

            var doc = await streamCollection.Find(ActiveStreamKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return new StreamConsumersInfoResult(StreamInfoResultStatus.NoStream, Array.Empty<StreamConsumerInfo>());
            }

            var groupDoc = doc.Groups.FirstOrDefault(x => string.Equals(x.Name, group, StringComparison.Ordinal));
            if (groupDoc == null)
            {
                return new StreamConsumersInfoResult(StreamInfoResultStatus.NoGroup, Array.Empty<StreamConsumerInfo>());
            }

            var nowUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var consumers = groupDoc.Consumers
                .Select(x =>
                {
                    var pendingCount = groupDoc.Pending.LongCount(p => string.Equals(p.Consumer, x.Name, StringComparison.Ordinal));
                    var idleMs = Math.Max(0, nowUnixMs - x.LastSeenUnixMs);
                    return new StreamConsumerInfo(x.Name, pendingCount, idleMs);
                })
                .ToArray();

            return new StreamConsumersInfoResult(StreamInfoResultStatus.Ok, consumers);
        }

        private async Task<StreamGroupReadResult> StreamGroupReadCoreAsync(string group, string consumer, string[] keys, string[] ids, int? count, TimeSpan? block, CancellationToken token)
        {
            if (keys.Length == 0 || keys.Length != ids.Length)
            {
                return new StreamGroupReadResult(StreamGroupReadResultStatus.InvalidId, Array.Empty<StreamReadResult>());
            }

            var nowUtc = DateTime.UtcNow;
            var nowUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var results = new List<StreamReadResult>(keys.Length);

            for (int i = 0; i < keys.Length; i++)
            {
                var key = keys[i];
                if (await IsStreamWrongTypeAsync(key, nowUtc, token))
                {
                    return new StreamGroupReadResult(StreamGroupReadResultStatus.WrongType, Array.Empty<StreamReadResult>());
                }

                var doc = await streamCollection.Find(ActiveStreamKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
                if (doc == null)
                {
                    return new StreamGroupReadResult(StreamGroupReadResultStatus.NoStream, Array.Empty<StreamReadResult>());
                }

                var groupDoc = doc.Groups.FirstOrDefault(x => string.Equals(x.Name, group, StringComparison.Ordinal));
                if (groupDoc == null)
                {
                    return new StreamGroupReadResult(StreamGroupReadResultStatus.NoGroup, Array.Empty<StreamReadResult>());
                }

                var id = ids[i];
                string fromId;
                if (id == ">")
                {
                    fromId = groupDoc.LastDeliveredId;
                }
                else
                {
                    if (!TryParseStreamId(id, out _, out _))
                    {
                        return new StreamGroupReadResult(StreamGroupReadResultStatus.InvalidId, Array.Empty<StreamReadResult>());
                    }

                    fromId = id;
                }

                if (!TryParseStreamId(fromId, out var fromMs, out var fromSeq))
                {
                    return new StreamGroupReadResult(StreamGroupReadResultStatus.InvalidId, Array.Empty<StreamReadResult>());
                }

                IEnumerable<StreamEntryDocument> query = doc.Entries
                    .Where(x => CompareStreamIds(x.Milliseconds, x.Sequence, fromMs, fromSeq) > 0)
                    .OrderBy(x => x.Milliseconds)
                    .ThenBy(x => x.Sequence);

                if (count.HasValue && count.Value > 0)
                {
                    query = query.Take(count.Value);
                }

                var selected = query.ToList();
                EnsureConsumer(groupDoc, consumer, nowUnixMs);

                if (selected.Count > 0)
                {
                    for (int entryIndex = 0; entryIndex < selected.Count; entryIndex++)
                    {
                        var entry = selected[entryIndex];
                        var pending = groupDoc.Pending.FirstOrDefault(x => string.Equals(x.Id, entry.Id, StringComparison.Ordinal));
                        if (pending == null)
                        {
                            pending = new StreamPendingDocument
                            {
                                Id = entry.Id,
                                Consumer = consumer,
                                LastDeliveryUnixMs = nowUnixMs,
                                DeliveryCount = 1
                            };
                            groupDoc.Pending.Add(pending);
                        }
                        else
                        {
                            pending.Consumer = consumer;
                            pending.LastDeliveryUnixMs = nowUnixMs;
                            pending.DeliveryCount++;
                        }

                        groupDoc.LastDeliveredId = entry.Id;
                    }

                    results.Add(new StreamReadResult(key, selected.Select(ToStreamEntry).ToArray()));
                }

                await streamCollection.ReplaceOneAsync(Builders<StreamDocument>.Filter.Eq(x => x.Id, doc.Id), doc, new ReplaceOptions(), token);
            }

            return new StreamGroupReadResult(StreamGroupReadResultStatus.Ok, results.ToArray());
        }

        private async Task<StreamAckResult> StreamAckCoreAsync(string key, string group, string[] ids, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsStreamWrongTypeAsync(key, nowUtc, token))
            {
                return new StreamAckResult(StreamAckResultStatus.WrongType, 0);
            }

            var doc = await streamCollection.Find(ActiveStreamKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return new StreamAckResult(StreamAckResultStatus.NoStream, 0);
            }

            var groupDoc = doc.Groups.FirstOrDefault(x => string.Equals(x.Name, group, StringComparison.Ordinal));
            if (groupDoc == null)
            {
                return new StreamAckResult(StreamAckResultStatus.NoGroup, 0);
            }

            if (ids.Length == 0)
            {
                return new StreamAckResult(StreamAckResultStatus.Ok, 0);
            }

            var idSet = new HashSet<string>(ids, StringComparer.Ordinal);
            var removed = groupDoc.Pending.RemoveAll(x => idSet.Contains(x.Id));
            if (removed > 0)
            {
                await streamCollection.ReplaceOneAsync(Builders<StreamDocument>.Filter.Eq(x => x.Id, doc.Id), doc, new ReplaceOptions(), token);
            }

            return new StreamAckResult(StreamAckResultStatus.Ok, removed);
        }

        private async Task<StreamPendingResult> StreamPendingCoreAsync(string key, string group, long? minIdleTimeMs, string? start, string? end, int? count, string? consumer, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsStreamWrongTypeAsync(key, nowUtc, token))
            {
                return new StreamPendingResult(StreamPendingResultStatus.WrongType, 0, null, null, Array.Empty<StreamPendingConsumerInfo>(), Array.Empty<StreamPendingEntry>());
            }

            var doc = await streamCollection.Find(ActiveStreamKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return new StreamPendingResult(StreamPendingResultStatus.NoStream, 0, null, null, Array.Empty<StreamPendingConsumerInfo>(), Array.Empty<StreamPendingEntry>());
            }

            var groupDoc = doc.Groups.FirstOrDefault(x => string.Equals(x.Name, group, StringComparison.Ordinal));
            if (groupDoc == null)
            {
                return new StreamPendingResult(StreamPendingResultStatus.NoGroup, 0, null, null, Array.Empty<StreamPendingConsumerInfo>(), Array.Empty<StreamPendingEntry>());
            }

            RemovePendingForMissingEntries(doc);

            var orderedAll = groupDoc.Pending
                .OrderBy(x => x.Id, Comparer<string>.Create(CompareStreamIds))
                .ToList();

            var totalCount = orderedAll.Count;
            var smallest = totalCount > 0 ? orderedAll[0].Id : null;
            var largest = totalCount > 0 ? orderedAll[^1].Id : null;

            var summaryOnly = minIdleTimeMs == null && start == null && end == null && count == null && consumer == null;
            if (summaryOnly)
            {
                var consumers = orderedAll
                    .GroupBy(x => x.Consumer, StringComparer.Ordinal)
                    .Select(x => new StreamPendingConsumerInfo(x.Key, x.LongCount()))
                    .ToArray();

                return new StreamPendingResult(StreamPendingResultStatus.Ok, totalCount, smallest, largest, consumers, Array.Empty<StreamPendingEntry>());
            }

            IEnumerable<StreamPendingDocument> filtered = orderedAll;
            var nowUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            if (minIdleTimeMs.HasValue && minIdleTimeMs.Value > 0)
            {
                filtered = filtered.Where(x => nowUnixMs - x.LastDeliveryUnixMs >= minIdleTimeMs.Value);
            }

            if (!string.IsNullOrEmpty(consumer))
            {
                filtered = filtered.Where(x => string.Equals(x.Consumer, consumer, StringComparison.Ordinal));
            }

            var startBound = start ?? "-";
            var endBound = end ?? "+";

            if (startBound != "-" && !TryParseStreamId(startBound, out _, out _))
            {
                return new StreamPendingResult(StreamPendingResultStatus.Ok, totalCount, smallest, largest, Array.Empty<StreamPendingConsumerInfo>(), Array.Empty<StreamPendingEntry>());
            }

            if (endBound != "+" && !TryParseStreamId(endBound, out _, out _))
            {
                return new StreamPendingResult(StreamPendingResultStatus.Ok, totalCount, smallest, largest, Array.Empty<StreamPendingConsumerInfo>(), Array.Empty<StreamPendingEntry>());
            }

            filtered = filtered.Where(x =>
                (startBound == "-" || CompareStreamIds(x.Id, startBound) >= 0) &&
                (endBound == "+" || CompareStreamIds(x.Id, endBound) <= 0));

            if (count.HasValue)
            {
                if (count.Value <= 0)
                {
                    filtered = Array.Empty<StreamPendingDocument>();
                }
                else
                {
                    filtered = filtered.Take(count.Value);
                }
            }

            var entries = filtered
                .Select(x => new StreamPendingEntry(x.Id, x.Consumer, Math.Max(0, nowUnixMs - x.LastDeliveryUnixMs), x.DeliveryCount))
                .ToArray();

            return new StreamPendingResult(StreamPendingResultStatus.Ok, totalCount, smallest, largest, Array.Empty<StreamPendingConsumerInfo>(), entries);
        }

        private async Task<StreamClaimResult> StreamClaimCoreAsync(string key, string group, string consumer, long minIdleTimeMs, string[] ids, long? idleMs, long? timeMs, long? retryCount, bool force, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsStreamWrongTypeAsync(key, nowUtc, token))
            {
                return new StreamClaimResult(StreamClaimResultStatus.WrongType, Array.Empty<StreamEntry>());
            }

            var doc = await streamCollection.Find(ActiveStreamKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return new StreamClaimResult(StreamClaimResultStatus.NoStream, Array.Empty<StreamEntry>());
            }

            var groupDoc = doc.Groups.FirstOrDefault(x => string.Equals(x.Name, group, StringComparison.Ordinal));
            if (groupDoc == null)
            {
                return new StreamClaimResult(StreamClaimResultStatus.NoGroup, Array.Empty<StreamEntry>());
            }

            if (ids.Length == 0)
            {
                return new StreamClaimResult(StreamClaimResultStatus.Ok, Array.Empty<StreamEntry>());
            }

            var nowUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            EnsureConsumer(groupDoc, consumer, nowUnixMs);

            var byId = doc.Entries.ToDictionary(x => x.Id, StringComparer.Ordinal);
            var claimed = new List<StreamEntry>();

            for (int i = 0; i < ids.Length; i++)
            {
                var id = ids[i];
                if (!TryParseStreamId(id, out _, out _))
                {
                    continue;
                }

                if (!byId.TryGetValue(id, out var entryDoc))
                {
                    continue;
                }

                var pending = groupDoc.Pending.FirstOrDefault(x => string.Equals(x.Id, id, StringComparison.Ordinal));
                if (pending == null)
                {
                    if (!force)
                    {
                        continue;
                    }

                    pending = new StreamPendingDocument
                    {
                        Id = id,
                        Consumer = consumer,
                        LastDeliveryUnixMs = nowUnixMs,
                        DeliveryCount = retryCount.HasValue && retryCount.Value >= 0 ? retryCount.Value : 1
                    };
                    groupDoc.Pending.Add(pending);
                }
                else
                {
                    var idle = Math.Max(0, nowUnixMs - pending.LastDeliveryUnixMs);
                    if (idle < minIdleTimeMs)
                    {
                        continue;
                    }

                    pending.Consumer = consumer;
                    pending.DeliveryCount = retryCount.HasValue && retryCount.Value >= 0 ? retryCount.Value : pending.DeliveryCount + 1;
                }

                if (timeMs.HasValue)
                {
                    pending.LastDeliveryUnixMs = timeMs.Value;
                }
                else if (idleMs.HasValue)
                {
                    pending.LastDeliveryUnixMs = nowUnixMs - idleMs.Value;
                }
                else
                {
                    pending.LastDeliveryUnixMs = nowUnixMs;
                }

                claimed.Add(ToStreamEntry(entryDoc));
            }

            await streamCollection.ReplaceOneAsync(Builders<StreamDocument>.Filter.Eq(x => x.Id, doc.Id), doc, new ReplaceOptions(), token);
            return new StreamClaimResult(StreamClaimResultStatus.Ok, claimed.ToArray());
        }

        private async Task<SetCountResult> SetAddCoreAsync(string key, byte[][] members, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsSetWrongTypeAsync(key, nowUtc, token))
            {
                return new SetCountResult(SetResultStatus.WrongType, 0);
            }

            if (members.Length == 0)
            {
                return new SetCountResult(SetResultStatus.Ok, 0);
            }

            for (int attempt = 0; attempt < 5; attempt++)
            {
                token.ThrowIfCancellationRequested();
                nowUtc = DateTime.UtcNow;
                var doc = await setCollection.Find(ActiveSetKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
                if (doc == null)
                {
                    try
                    {
                        var unique = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                        for (int i = 0; i < members.Length; i++)
                        {
                            unique[ToMemberKey(members[i])] = members[i];
                        }

                        var created = new SetDocument
                        {
                            Key = key,
                            ExpireAt = null,
                            Members = unique.Select(x => new SetMemberDocument { MemberKey = x.Key, Member = x.Value }).ToList()
                        };

                        await setCollection.InsertOneAsync(created, null, token);
                        return new SetCountResult(SetResultStatus.Ok, created.Members.Count);
                    }
                    catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
                    {
                        continue;
                    }
                }

                var existing = new HashSet<string>(doc.Members.Select(x => x.MemberKey), StringComparer.Ordinal);
                long added = 0;
                for (int i = 0; i < members.Length; i++)
                {
                    var memberKey = ToMemberKey(members[i]);
                    if (existing.Add(memberKey))
                    {
                        doc.Members.Add(new SetMemberDocument { MemberKey = memberKey, Member = members[i] });
                        added++;
                    }
                }

                if (added == 0)
                {
                    return new SetCountResult(SetResultStatus.Ok, 0);
                }

                var replace = await setCollection.ReplaceOneAsync(
                    Builders<SetDocument>.Filter.Eq(x => x.Id, doc.Id),
                    doc,
                    new ReplaceOptions(),
                    token);

                if (replace.MatchedCount > 0)
                {
                    return new SetCountResult(SetResultStatus.Ok, added);
                }
            }

            return new SetCountResult(SetResultStatus.Ok, 0);
        }

        private async Task<SetCountResult> SetRemoveCoreAsync(string key, byte[][] members, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsSetWrongTypeAsync(key, nowUtc, token))
            {
                return new SetCountResult(SetResultStatus.WrongType, 0);
            }

            if (members.Length == 0)
            {
                return new SetCountResult(SetResultStatus.Ok, 0);
            }

            var doc = await setCollection.Find(ActiveSetKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null || doc.Members.Count == 0)
            {
                return new SetCountResult(SetResultStatus.Ok, 0);
            }

            var removeKeys = new HashSet<string>(members.Select(ToMemberKey), StringComparer.Ordinal);
            var original = doc.Members.Count;
            doc.Members.RemoveAll(x => removeKeys.Contains(x.MemberKey));
            var removed = original - doc.Members.Count;
            if (removed <= 0)
            {
                return new SetCountResult(SetResultStatus.Ok, 0);
            }

            if (doc.Members.Count == 0)
            {
                await setCollection.DeleteOneAsync(Builders<SetDocument>.Filter.Eq(x => x.Id, doc.Id), token);
            }
            else
            {
                await setCollection.ReplaceOneAsync(Builders<SetDocument>.Filter.Eq(x => x.Id, doc.Id), doc, new ReplaceOptions(), token);
            }

            return new SetCountResult(SetResultStatus.Ok, removed);
        }

        private async Task<SetMembersResult> SetMembersCoreAsync(string key, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsSetWrongTypeAsync(key, nowUtc, token))
            {
                return new SetMembersResult(SetResultStatus.WrongType, Array.Empty<byte[]>());
            }

            var doc = await setCollection.Find(ActiveSetKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null || doc.Members.Count == 0)
            {
                return new SetMembersResult(SetResultStatus.Ok, Array.Empty<byte[]>());
            }

            return new SetMembersResult(SetResultStatus.Ok, doc.Members.Select(x => x.Member).ToArray());
        }

        private async Task<SetCountResult> SetCardinalityCoreAsync(string key, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsSetWrongTypeAsync(key, nowUtc, token))
            {
                return new SetCountResult(SetResultStatus.WrongType, 0);
            }

            var doc = await setCollection.Find(ActiveSetKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            return new SetCountResult(SetResultStatus.Ok, doc?.Members.Count ?? 0);
        }

        private async Task<SortedSetCountResult> SortedSetAddCoreAsync(string key, SortedSetEntry[] entries, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsSortedSetWrongTypeAsync(key, nowUtc, token))
            {
                return new SortedSetCountResult(SortedSetResultStatus.WrongType, 0);
            }

            if (entries.Length == 0)
            {
                return new SortedSetCountResult(SortedSetResultStatus.Ok, 0);
            }

            for (int attempt = 0; attempt < 5; attempt++)
            {
                token.ThrowIfCancellationRequested();
                nowUtc = DateTime.UtcNow;
                var doc = await sortedSetCollection.Find(ActiveSortedSetKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
                if (doc == null)
                {
                    try
                    {
                        var map = new Dictionary<string, SortedSetMemberDocument>(StringComparer.Ordinal);
                        for (int i = 0; i < entries.Length; i++)
                        {
                            var memberKey = ToMemberKey(entries[i].Member);
                            map[memberKey] = new SortedSetMemberDocument
                            {
                                MemberKey = memberKey,
                                Member = entries[i].Member,
                                Score = entries[i].Score
                            };
                        }

                        var created = new SortedSetDocument
                        {
                            Key = key,
                            ExpireAt = null,
                            Members = SortSortedSetMembers(map.Values)
                        };

                        await sortedSetCollection.InsertOneAsync(created, null, token);
                        return new SortedSetCountResult(SortedSetResultStatus.Ok, created.Members.Count);
                    }
                    catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
                    {
                        continue;
                    }
                }

                var byKey = doc.Members.ToDictionary(x => x.MemberKey, x => x, StringComparer.Ordinal);
                long added = 0;
                for (int i = 0; i < entries.Length; i++)
                {
                    var memberKey = ToMemberKey(entries[i].Member);
                    if (byKey.TryGetValue(memberKey, out var existing))
                    {
                        existing.Score = entries[i].Score;
                        existing.Member = entries[i].Member;
                    }
                    else
                    {
                        var createdMember = new SortedSetMemberDocument
                        {
                            MemberKey = memberKey,
                            Member = entries[i].Member,
                            Score = entries[i].Score
                        };
                        doc.Members.Add(createdMember);
                        byKey[memberKey] = createdMember;
                        added++;
                    }
                }

                doc.Members = SortSortedSetMembers(doc.Members);
                var replace = await sortedSetCollection.ReplaceOneAsync(
                    Builders<SortedSetDocument>.Filter.Eq(x => x.Id, doc.Id),
                    doc,
                    new ReplaceOptions(),
                    token);

                if (replace.MatchedCount > 0)
                {
                    return new SortedSetCountResult(SortedSetResultStatus.Ok, added);
                }
            }

            return new SortedSetCountResult(SortedSetResultStatus.Ok, 0);
        }

        private async Task<SortedSetCountResult> SortedSetRemoveCoreAsync(string key, byte[][] members, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsSortedSetWrongTypeAsync(key, nowUtc, token))
            {
                return new SortedSetCountResult(SortedSetResultStatus.WrongType, 0);
            }

            if (members.Length == 0)
            {
                return new SortedSetCountResult(SortedSetResultStatus.Ok, 0);
            }

            var doc = await sortedSetCollection.Find(ActiveSortedSetKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null || doc.Members.Count == 0)
            {
                return new SortedSetCountResult(SortedSetResultStatus.Ok, 0);
            }

            var removeKeys = new HashSet<string>(members.Select(ToMemberKey), StringComparer.Ordinal);
            var original = doc.Members.Count;
            doc.Members.RemoveAll(x => removeKeys.Contains(x.MemberKey));
            var removed = original - doc.Members.Count;

            if (removed <= 0)
            {
                return new SortedSetCountResult(SortedSetResultStatus.Ok, 0);
            }

            if (doc.Members.Count == 0)
            {
                await sortedSetCollection.DeleteOneAsync(Builders<SortedSetDocument>.Filter.Eq(x => x.Id, doc.Id), token);
            }
            else
            {
                await sortedSetCollection.ReplaceOneAsync(Builders<SortedSetDocument>.Filter.Eq(x => x.Id, doc.Id), doc, new ReplaceOptions(), token);
            }

            return new SortedSetCountResult(SortedSetResultStatus.Ok, removed);
        }

        private async Task<SortedSetRangeResult> SortedSetRangeCoreAsync(string key, int start, int stop, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsSortedSetWrongTypeAsync(key, nowUtc, token))
            {
                return new SortedSetRangeResult(SortedSetResultStatus.WrongType, Array.Empty<SortedSetEntry>());
            }

            var doc = await sortedSetCollection.Find(ActiveSortedSetKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null || doc.Members.Count == 0)
            {
                return new SortedSetRangeResult(SortedSetResultStatus.Ok, Array.Empty<SortedSetEntry>());
            }

            var sorted = SortSortedSetMembers(doc.Members);
            if (!TryNormalizeRange(sorted.Count, start, stop, out var from, out var to))
            {
                return new SortedSetRangeResult(SortedSetResultStatus.Ok, Array.Empty<SortedSetEntry>());
            }

            var entries = sorted
                .Skip(from)
                .Take(to - from + 1)
                .Select(x => new SortedSetEntry(x.Member, x.Score))
                .ToArray();

            return new SortedSetRangeResult(SortedSetResultStatus.Ok, entries);
        }

        private async Task<SortedSetCountResult> SortedSetCardinalityCoreAsync(string key, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsSortedSetWrongTypeAsync(key, nowUtc, token))
            {
                return new SortedSetCountResult(SortedSetResultStatus.WrongType, 0);
            }

            var doc = await sortedSetCollection.Find(ActiveSortedSetKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            return new SortedSetCountResult(SortedSetResultStatus.Ok, doc?.Members.Count ?? 0);
        }

        private async Task<SortedSetScoreResult> SortedSetScoreCoreAsync(string key, byte[] member, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsSortedSetWrongTypeAsync(key, nowUtc, token))
            {
                return new SortedSetScoreResult(SortedSetResultStatus.WrongType, null);
            }

            var doc = await sortedSetCollection.Find(ActiveSortedSetKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return new SortedSetScoreResult(SortedSetResultStatus.Ok, null);
            }

            var memberKey = ToMemberKey(member);
            var existing = doc.Members.FirstOrDefault(x => string.Equals(x.MemberKey, memberKey, StringComparison.Ordinal));
            return new SortedSetScoreResult(SortedSetResultStatus.Ok, existing?.Score);
        }

        private async Task<SortedSetRangeResult> SortedSetRangeByScoreCoreAsync(string key, double minScore, double maxScore, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsSortedSetWrongTypeAsync(key, nowUtc, token))
            {
                return new SortedSetRangeResult(SortedSetResultStatus.WrongType, Array.Empty<SortedSetEntry>());
            }

            var doc = await sortedSetCollection.Find(ActiveSortedSetKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null || doc.Members.Count == 0)
            {
                return new SortedSetRangeResult(SortedSetResultStatus.Ok, Array.Empty<SortedSetEntry>());
            }

            var entries = SortSortedSetMembers(doc.Members)
                .Where(x => x.Score >= minScore && x.Score <= maxScore)
                .Select(x => new SortedSetEntry(x.Member, x.Score))
                .ToArray();

            return new SortedSetRangeResult(SortedSetResultStatus.Ok, entries);
        }

        private async Task<SortedSetScoreResult> SortedSetIncrementCoreAsync(string key, double increment, byte[] member, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsSortedSetWrongTypeAsync(key, nowUtc, token))
            {
                return new SortedSetScoreResult(SortedSetResultStatus.WrongType, null);
            }

            var memberKey = ToMemberKey(member);
            for (int attempt = 0; attempt < 5; attempt++)
            {
                token.ThrowIfCancellationRequested();
                nowUtc = DateTime.UtcNow;
                var doc = await sortedSetCollection.Find(ActiveSortedSetKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
                if (doc == null)
                {
                    try
                    {
                        var score = increment;
                        var created = new SortedSetDocument
                        {
                            Key = key,
                            ExpireAt = null,
                            Members = new List<SortedSetMemberDocument>
                            {
                                new SortedSetMemberDocument { MemberKey = memberKey, Member = member, Score = score }
                            }
                        };

                        await sortedSetCollection.InsertOneAsync(created, null, token);
                        return new SortedSetScoreResult(SortedSetResultStatus.Ok, score);
                    }
                    catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
                    {
                        continue;
                    }
                }

                var existing = doc.Members.FirstOrDefault(x => string.Equals(x.MemberKey, memberKey, StringComparison.Ordinal));
                if (existing == null)
                {
                    existing = new SortedSetMemberDocument { MemberKey = memberKey, Member = member, Score = increment };
                    doc.Members.Add(existing);
                }
                else
                {
                    existing.Member = member;
                    existing.Score += increment;
                }

                doc.Members = SortSortedSetMembers(doc.Members);
                var replace = await sortedSetCollection.ReplaceOneAsync(
                    Builders<SortedSetDocument>.Filter.Eq(x => x.Id, doc.Id),
                    doc,
                    new ReplaceOptions(),
                    token);

                if (replace.MatchedCount > 0)
                {
                    return new SortedSetScoreResult(SortedSetResultStatus.Ok, existing.Score);
                }
            }

            return new SortedSetScoreResult(SortedSetResultStatus.Ok, null);
        }

        private async Task<SortedSetCountResult> SortedSetCountByScoreCoreAsync(string key, double minScore, double maxScore, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsSortedSetWrongTypeAsync(key, nowUtc, token))
            {
                return new SortedSetCountResult(SortedSetResultStatus.WrongType, 0);
            }

            var doc = await sortedSetCollection.Find(ActiveSortedSetKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null || doc.Members.Count == 0)
            {
                return new SortedSetCountResult(SortedSetResultStatus.Ok, 0);
            }

            var count = doc.Members.LongCount(x => x.Score >= minScore && x.Score <= maxScore);
            return new SortedSetCountResult(SortedSetResultStatus.Ok, count);
        }

        private async Task<SortedSetRankResult> SortedSetRankCoreAsync(string key, byte[] member, bool reverse, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsSortedSetWrongTypeAsync(key, nowUtc, token))
            {
                return new SortedSetRankResult(SortedSetResultStatus.WrongType, null);
            }

            var doc = await sortedSetCollection.Find(ActiveSortedSetKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null || doc.Members.Count == 0)
            {
                return new SortedSetRankResult(SortedSetResultStatus.Ok, null);
            }

            var sorted = SortSortedSetMembers(doc.Members);
            var memberKey = ToMemberKey(member);
            var index = sorted.FindIndex(x => string.Equals(x.MemberKey, memberKey, StringComparison.Ordinal));
            if (index < 0)
            {
                return new SortedSetRankResult(SortedSetResultStatus.Ok, null);
            }

            long rank = reverse ? sorted.Count - 1 - index : index;
            return new SortedSetRankResult(SortedSetResultStatus.Ok, rank);
        }

        private async Task<SortedSetRemoveRangeResult> SortedSetRemoveRangeByScoreCoreAsync(string key, double minScore, double maxScore, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsSortedSetWrongTypeAsync(key, nowUtc, token))
            {
                return new SortedSetRemoveRangeResult(SortedSetResultStatus.WrongType, 0);
            }

            var doc = await sortedSetCollection.Find(ActiveSortedSetKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null || doc.Members.Count == 0)
            {
                return new SortedSetRemoveRangeResult(SortedSetResultStatus.Ok, 0);
            }

            var original = doc.Members.Count;
            doc.Members.RemoveAll(x => x.Score >= minScore && x.Score <= maxScore);
            var removed = original - doc.Members.Count;
            if (removed <= 0)
            {
                return new SortedSetRemoveRangeResult(SortedSetResultStatus.Ok, 0);
            }

            if (doc.Members.Count == 0)
            {
                await sortedSetCollection.DeleteOneAsync(Builders<SortedSetDocument>.Filter.Eq(x => x.Id, doc.Id), token);
            }
            else
            {
                await sortedSetCollection.ReplaceOneAsync(Builders<SortedSetDocument>.Filter.Eq(x => x.Id, doc.Id), doc, new ReplaceOptions(), token);
            }

            return new SortedSetRemoveRangeResult(SortedSetResultStatus.Ok, removed);
        }

        private static void ApplyPushValues(List<byte[]> destination, byte[][] values, bool left)
        {
            if (left)
            {
                for (int i = 0; i < values.Length; i++)
                {
                    destination.Insert(0, values[i]);
                }
            }
            else
            {
                for (int i = 0; i < values.Length; i++)
                {
                    destination.Add(values[i]);
                }
            }
        }

        private static int NormalizeIndex(int length, int index)
        {
            var normalized = index;
            if (normalized < 0)
            {
                normalized += length;
            }

            return normalized;
        }

        private static bool TryNormalizeRange(int length, int start, int stop, out int from, out int to)
        {
            from = start < 0 ? length + start : start;
            to = stop < 0 ? length + stop : stop;

            if (from < 0)
            {
                from = 0;
            }

            if (to >= length)
            {
                to = length - 1;
            }

            if (length == 0 || from >= length || to < 0 || from > to)
            {
                return false;
            }

            return true;
        }

        private async Task<ListPushResult> ListPushCoreAsync(string key, byte[][] values, bool left, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsListWrongTypeAsync(key, nowUtc, token))
            {
                return new ListPushResult(ListResultStatus.WrongType, 0);
            }

            for (int attempt = 0; attempt < 5; attempt++)
            {
                token.ThrowIfCancellationRequested();
                nowUtc = DateTime.UtcNow;
                var doc = await listCollection.Find(ActiveListKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
                if (doc == null)
                {
                    try
                    {
                        var created = new ListDocument { Key = key, ExpireAt = null };
                        ApplyPushValues(created.Values, values, left);
                        await listCollection.InsertOneAsync(created, null, token);
                        return new ListPushResult(ListResultStatus.Ok, created.Values.Count);
                    }
                    catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
                    {
                        continue;
                    }
                }

                ApplyPushValues(doc.Values, values, left);
                var result = await listCollection.ReplaceOneAsync(Builders<ListDocument>.Filter.Eq(x => x.Id, doc.Id), doc, new ReplaceOptions(), token);
                if (result.MatchedCount > 0)
                {
                    return new ListPushResult(ListResultStatus.Ok, doc.Values.Count);
                }
            }

            return new ListPushResult(ListResultStatus.Ok, 0);
        }

        private async Task<ListPopResult> ListPopCoreAsync(string key, bool left, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsListWrongTypeAsync(key, nowUtc, token))
            {
                return new ListPopResult(ListResultStatus.WrongType, null);
            }

            var doc = await listCollection.Find(ActiveListKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null || doc.Values.Count == 0)
            {
                return new ListPopResult(ListResultStatus.Ok, null);
            }

            var index = left ? 0 : doc.Values.Count - 1;
            var value = doc.Values[index];
            doc.Values.RemoveAt(index);

            if (doc.Values.Count == 0)
            {
                await listCollection.DeleteOneAsync(Builders<ListDocument>.Filter.Eq(x => x.Id, doc.Id), token);
            }
            else
            {
                await listCollection.ReplaceOneAsync(Builders<ListDocument>.Filter.Eq(x => x.Id, doc.Id), doc, new ReplaceOptions(), token);
            }

            return new ListPopResult(ListResultStatus.Ok, value);
        }

        private async Task<ListRangeResult> ListRangeCoreAsync(string key, int start, int stop, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsListWrongTypeAsync(key, nowUtc, token))
            {
                return new ListRangeResult(ListResultStatus.WrongType, Array.Empty<byte[]>());
            }

            var doc = await listCollection.Find(ActiveListKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null || doc.Values.Count == 0)
            {
                return new ListRangeResult(ListResultStatus.Ok, Array.Empty<byte[]>());
            }

            if (!TryNormalizeRange(doc.Values.Count, start, stop, out var from, out var to))
            {
                return new ListRangeResult(ListResultStatus.Ok, Array.Empty<byte[]>());
            }

            var length = to - from + 1;
            var values = doc.Values.Skip(from).Take(length).ToArray();
            return new ListRangeResult(ListResultStatus.Ok, values);
        }

        private async Task<ListLengthResult> ListLengthCoreAsync(string key, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsListWrongTypeAsync(key, nowUtc, token))
            {
                return new ListLengthResult(ListResultStatus.WrongType, 0);
            }

            var doc = await listCollection.Find(ActiveListKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            return new ListLengthResult(ListResultStatus.Ok, doc?.Values.Count ?? 0);
        }

        private async Task<ListIndexResult> ListIndexCoreAsync(string key, int index, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsListWrongTypeAsync(key, nowUtc, token))
            {
                return new ListIndexResult(ListResultStatus.WrongType, null);
            }

            var doc = await listCollection.Find(ActiveListKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null || doc.Values.Count == 0)
            {
                return new ListIndexResult(ListResultStatus.Ok, null);
            }

            var normalizedIndex = NormalizeIndex(doc.Values.Count, index);
            if (normalizedIndex < 0 || normalizedIndex >= doc.Values.Count)
            {
                return new ListIndexResult(ListResultStatus.Ok, null);
            }

            return new ListIndexResult(ListResultStatus.Ok, doc.Values[normalizedIndex]);
        }

        private async Task<ListSetResult> ListSetCoreAsync(string key, int index, byte[] value, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsListWrongTypeAsync(key, nowUtc, token))
            {
                return new ListSetResult(ListSetResultStatus.WrongType);
            }

            var doc = await listCollection.Find(ActiveListKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null || doc.Values.Count == 0)
            {
                return new ListSetResult(ListSetResultStatus.OutOfRange);
            }

            var normalizedIndex = NormalizeIndex(doc.Values.Count, index);
            if (normalizedIndex < 0 || normalizedIndex >= doc.Values.Count)
            {
                return new ListSetResult(ListSetResultStatus.OutOfRange);
            }

            doc.Values[normalizedIndex] = value;
            await listCollection.ReplaceOneAsync(Builders<ListDocument>.Filter.Eq(x => x.Id, doc.Id), doc, new ReplaceOptions(), token);
            return new ListSetResult(ListSetResultStatus.Ok);
        }

        private async Task<ListResultStatus> ListTrimCoreAsync(string key, int start, int stop, CancellationToken token)
        {
            var nowUtc = DateTime.UtcNow;
            if (await IsListWrongTypeAsync(key, nowUtc, token))
            {
                return ListResultStatus.WrongType;
            }

            var doc = await listCollection.Find(ActiveListKeyFilter(key, nowUtc)).FirstOrDefaultAsync(token);
            if (doc == null)
            {
                return ListResultStatus.Ok;
            }

            if (!TryNormalizeRange(doc.Values.Count, start, stop, out var from, out var to))
            {
                await listCollection.DeleteOneAsync(Builders<ListDocument>.Filter.Eq(x => x.Id, doc.Id), token);
                return ListResultStatus.Ok;
            }

            var length = to - from + 1;
            doc.Values = doc.Values.Skip(from).Take(length).ToList();
            await listCollection.ReplaceOneAsync(Builders<ListDocument>.Filter.Eq(x => x.Id, doc.Id), doc, new ReplaceOptions(), token);
            return ListResultStatus.Ok;
        }
    }
}
