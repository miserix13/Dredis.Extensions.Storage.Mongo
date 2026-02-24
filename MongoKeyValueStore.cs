using Dredis.Abstractions.Storage;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Globalization;
using System.Text;

namespace Dredis.Extensions.Storage.Mongo
{
    public class MongoKeyValueStore : IKeyValueStore
    {

        private readonly IMongoDatabase database;
        private readonly IMongoCollection<KeyValueDocument> collection;
    private readonly IMongoCollection<HashDocument> hashCollection;
    private readonly IMongoCollection<ListDocument> listCollection;

        public MongoKeyValueStore(MongoClient mongoClient, string databaseName = "dredis", string collectionName = "kvstore") : base()
        {
            this.database = mongoClient.GetDatabase(databaseName);
            this.collection = this.database.GetCollection<KeyValueDocument>(collectionName ?? "kvstore");
            this.hashCollection = this.database.GetCollection<HashDocument>($"{collectionName ?? "kvstore"}_hash");
            this.listCollection = this.database.GetCollection<ListDocument>($"{collectionName ?? "kvstore"}_list");

            var keyIndexKeys = Builders<KeyValueDocument>.IndexKeys.Ascending(x => x.Key);
            var keyIndexModel = new CreateIndexModel<KeyValueDocument>(keyIndexKeys, new CreateIndexOptions { Unique = true });
            this.collection.Indexes.CreateOne(keyIndexModel);

            var hashKeyIndexKeys = Builders<HashDocument>.IndexKeys.Ascending(x => x.Key);
            var hashKeyIndexModel = new CreateIndexModel<HashDocument>(hashKeyIndexKeys, new CreateIndexOptions { Unique = true });
            this.hashCollection.Indexes.CreateOne(hashKeyIndexModel);

            var listKeyIndexKeys = Builders<ListDocument>.IndexKeys.Ascending(x => x.Key);
            var listKeyIndexModel = new CreateIndexModel<ListDocument>(listKeyIndexKeys, new CreateIndexOptions { Unique = true });
            this.listCollection.Indexes.CreateOne(listKeyIndexModel);

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
            return result.DeletedCount + hashResult.DeletedCount + listResult.DeletedCount;
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
            return listCount > 0;
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
            return listResult.MatchedCount > 0;
        }

        public Task<ProbabilisticBoolResult> BloomAddAsync(string key, byte[] element, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticBoolResult> BloomExistsAsync(string key, byte[] element, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticInfoResult> BloomInfoAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticArrayResult> BloomMAddAsync(string key, byte[][] elements, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticArrayResult> BloomMExistsAsync(string key, byte[][] elements, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticResultStatus> BloomReserveAsync(string key, double errorRate, long capacity, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<long> CleanUpExpiredKeysAsync(CancellationToken token = default)
        {
            return CleanUpExpiredKeysCoreAsync(token);
        }

        public Task<ProbabilisticBoolResult> CuckooAddAsync(string key, byte[] item, bool noCreate, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticBoolResult> CuckooAddNxAsync(string key, byte[] item, bool noCreate, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticCountResult> CuckooCountAsync(string key, byte[] item, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticBoolResult> CuckooDeleteAsync(string key, byte[] item, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticBoolResult> CuckooExistsAsync(string key, byte[] item, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticInfoResult> CuckooInfoAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticResultStatus> CuckooReserveAsync(string key, long capacity, CancellationToken token = default)
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }

        public Task<HyperLogLogCountResult> HyperLogLogCountAsync(string[] keys, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<HyperLogLogMergeResult> HyperLogLogMergeAsync(string destinationKey, string[] sourceKeys, CancellationToken token = default)
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }

        public Task<SetCountResult> SetCardinalityAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<bool> SetManyAsync(KeyValuePair<string, byte[]>[] items, CancellationToken token = default)
        {
            return SetManyCoreAsync(items, token);
        }

        public Task<SetMembersResult> SetMembersAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<SetCountResult> SetRemoveAsync(string key, byte[][] members, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<SortedSetCountResult> SortedSetAddAsync(string key, SortedSetEntry[] entries, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<SortedSetCountResult> SortedSetCardinalityAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<SortedSetCountResult> SortedSetCountByScoreAsync(string key, double minScore, double maxScore, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<SortedSetScoreResult> SortedSetIncrementAsync(string key, double increment, byte[] member, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<SortedSetRangeResult> SortedSetRangeAsync(string key, int start, int stop, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<SortedSetRangeResult> SortedSetRangeByScoreAsync(string key, double minScore, double maxScore, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<SortedSetRankResult> SortedSetRankAsync(string key, byte[] member, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<SortedSetCountResult> SortedSetRemoveAsync(string key, byte[][] members, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<SortedSetRemoveRangeResult> SortedSetRemoveRangeByScoreAsync(string key, double minScore, double maxScore, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<SortedSetRankResult> SortedSetReverseRankAsync(string key, byte[] member, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<SortedSetScoreResult> SortedSetScoreAsync(string key, byte[] member, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<StreamAckResult> StreamAckAsync(string key, string group, string[] ids, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<string?> StreamAddAsync(string key, string id, KeyValuePair<string, byte[]>[] fields, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<StreamClaimResult> StreamClaimAsync(string key, string group, string consumer, long minIdleTimeMs, string[] ids, long? idleMs = null, long? timeMs = null, long? retryCount = null, bool force = false, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<StreamConsumersInfoResult> StreamConsumersInfoAsync(string key, string group, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<long> StreamDeleteAsync(string key, string[] ids, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<StreamGroupCreateResult> StreamGroupCreateAsync(string key, string group, string startId, bool mkStream, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<StreamGroupDelConsumerResult> StreamGroupDelConsumerAsync(string key, string group, string consumer, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<StreamGroupDestroyResult> StreamGroupDestroyAsync(string key, string group, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<StreamGroupReadResult> StreamGroupReadAsync(string group, string consumer, string[] keys, string[] ids, int? count, TimeSpan? block, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<StreamGroupSetIdResultStatus> StreamGroupSetIdAsync(string key, string group, string lastId, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<StreamGroupsInfoResult> StreamGroupsInfoAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<StreamInfoResult> StreamInfoAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<string?> StreamLastIdAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<long> StreamLengthAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<StreamPendingResult> StreamPendingAsync(string key, string group, long? minIdleTimeMs = null, string? start = null, string? end = null, int? count = null, string? consumer = null, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<StreamEntry[]> StreamRangeAsync(string key, string start, string end, int? count, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<StreamEntry[]> StreamRangeReverseAsync(string key, string start, string end, int? count, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<StreamReadResult[]> StreamReadAsync(string[] keys, string[] ids, int? count, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<StreamSetIdResultStatus> StreamSetIdAsync(string key, string lastId, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<long> StreamTrimAsync(string key, int? maxLength = null, string? minId = null, bool approximate = false, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticResultStatus> TDigestAddAsync(string key, double[] values, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticDoubleArrayResult> TDigestByRankAsync(string key, long[] ranks, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticDoubleArrayResult> TDigestByRevRankAsync(string key, long[] ranks, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticDoubleArrayResult> TDigestCdfAsync(string key, double[] values, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticResultStatus> TDigestCreateAsync(string key, int compression, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticInfoResult> TDigestInfoAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticDoubleResult> TDigestMaxAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticDoubleResult> TDigestMinAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticDoubleArrayResult> TDigestQuantileAsync(string key, double[] quantiles, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticArrayResult> TDigestRankAsync(string key, double[] values, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticResultStatus> TDigestResetAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticArrayResult> TDigestRevRankAsync(string key, double[] values, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ProbabilisticDoubleResult> TDigestTrimmedMeanAsync(string key, double lowerQuantile, double upperQuantile, CancellationToken token = default)
        {
            throw new NotImplementedException();
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
            return result.DeletedCount + hashResult.DeletedCount + listResult.DeletedCount;
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
                await listCollection.CountDocumentsAsync(ActiveListKeyFilter(key, nowUtc), null, token) > 0)
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
            return hashCount > 0;
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
