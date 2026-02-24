using Dredis.Abstractions.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Dredis.Extensions.Storage.Mongo
{
    public class MongoKeyValueStore(MongoClient mongoClient, string databaseName = "dredis") : IKeyValueStore
    {

        private readonly IMongoDatabase database = mongoClient.GetDatabase(databaseName);
        private readonly IMongoCollection<KeyValueDocument> collection;

        public MongoKeyValueStore(MongoClient mongoClient, string databaseName = "dredis") : this(mongoClient, databaseName, null) { }

        public MongoKeyValueStore(MongoClient mongoClient, string databaseName, string? collectionName = null) : this(mongoClient)
        {
            database = mongoClient.GetDatabase(databaseName);
            collection = database.GetCollection<KeyValueDocument>(collectionName ?? "kvstore");
            // Ensure TTL index on ExpireAt
            var indexKeys = Builders<KeyValueDocument>.IndexKeys.Ascending(x => x.ExpireAt);
            var indexModel = new CreateIndexModel<KeyValueDocument>(indexKeys, new CreateIndexOptions { ExpireAfter = TimeSpan.Zero });
            collection.Indexes.CreateOne(indexModel);
        }

        private class KeyValueDocument
        {
            public ObjectId Id { get; set; }
            public string Key { get; set; } = null!;
            public byte[] Value { get; set; } = null!;
            public DateTime? ExpireAt { get; set; }
        }

        private FilterDefinition<KeyValueDocument> KeyFilter(string key) => Builders<KeyValueDocument>.Filter.Eq(x => x.Key, key);

        public async Task<bool> SetAsync(string key, byte[] value, TimeSpan? expiration, SetCondition condition, CancellationToken token = default)
        {
            var filter = KeyFilter(key);
            var update = Builders<KeyValueDocument>.Update
                .Set(x => x.Value, value)
                .Set(x => x.Key, key)
                .Set(x => x.ExpireAt, expiration.HasValue ? DateTime.UtcNow.Add(expiration.Value) : null);

            UpdateOptions options = new() { IsUpsert = true };
            if (condition == SetCondition.Nx)
            {
                // Only insert if not exists
                var doc = new KeyValueDocument { Key = key, Value = value, ExpireAt = expiration.HasValue ? DateTime.UtcNow.Add(expiration.Value) : null };
                var result = await collection.InsertOneAsync(doc, null, token);
                return true;
            }
            else if (condition == SetCondition.Xx)
            {
                // Only update if exists
                var updateResult = await collection.UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = false }, token);
                return updateResult.ModifiedCount > 0;
            }
            else
            {
                // Always upsert
                var updateResult = await collection.UpdateOneAsync(filter, update, options, token);
                return updateResult.ModifiedCount > 0 || updateResult.UpsertedId != null;
            }
        }

        public async Task<byte[]?> GetAsync(string key, CancellationToken token = default)
        {
            var filter = KeyFilter(key);
            var doc = await collection.Find(filter).FirstOrDefaultAsync(token);
            if (doc == null || (doc.ExpireAt.HasValue && doc.ExpireAt.Value < DateTime.UtcNow))
                return null;
            return doc.Value;
        }

        public async Task<long> DeleteAsync(string[] keys, CancellationToken token = default)
        {
            var filter = Builders<KeyValueDocument>.Filter.In(x => x.Key, keys);
            var result = await collection.DeleteManyAsync(filter, token);
            return result.DeletedCount;
        }

        public async Task<bool> ExistsAsync(string key, CancellationToken token = default)
        {
            var filter = KeyFilter(key);
            var count = await collection.CountDocumentsAsync(filter, null, token);
            return count > 0;
        }

        public async Task<long> ExistsAsync(string[] keys, CancellationToken token = default)
        {
            var filter = Builders<KeyValueDocument>.Filter.In(x => x.Key, keys);
            var count = await collection.CountDocumentsAsync(filter, null, token);
            return count;
        }

        public async Task<bool> ExpireAsync(string key, TimeSpan expiration, CancellationToken token = default)
        {
            var filter = KeyFilter(key);
            var update = Builders<KeyValueDocument>.Update.Set(x => x.ExpireAt, DateTime.UtcNow.Add(expiration));
            var result = await collection.UpdateOneAsync(filter, update, null, token);
            return result.ModifiedCount > 0;
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
            throw new NotImplementedException();
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

        public Task<long> DeleteAsync(string[] keys, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<bool> ExistsAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<long> ExistsAsync(string[] keys, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<bool> ExpireAsync(string key, TimeSpan expiration, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<byte[]?> GetAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<byte[]?[]> GetManyAsync(string[] keys, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<long> HashDeleteAsync(string key, string[] fields, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<KeyValuePair<string, byte[]>[]> HashGetAllAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<byte[]?> HashGetAsync(string key, string field, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<bool> HashSetAsync(string key, string field, byte[] value, CancellationToken token = default)
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }

        public Task<ListLengthResult> ListLengthAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ListPopResult> ListPopAsync(string key, bool left, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ListPushResult> ListPushAsync(string key, byte[][] values, bool left, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ListRangeResult> ListRangeAsync(string key, int start, int stop, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ListSetResult> ListSetAsync(string key, int index, byte[] value, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<ListResultStatus> ListTrimAsync(string key, int start, int stop, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<bool> PExpireAsync(string key, TimeSpan expiration, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<long> PttlAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<SetCountResult> SetAddAsync(string key, byte[][] members, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public async Task<bool> SetAsync(string key, byte[] value, TimeSpan? expiration, SetCondition condition, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<SetCountResult> SetCardinalityAsync(string key, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<bool> SetManyAsync(KeyValuePair<string, byte[]>[] items, CancellationToken token = default)
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
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
    }
}
