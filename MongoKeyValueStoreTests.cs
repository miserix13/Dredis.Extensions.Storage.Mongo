using Dredis.Abstractions.Storage;
using Dredis.Extensions.Storage.Mongo;
using Mongo2Go;
using MongoDB.Driver;
using System.Text;
using Xunit;

public sealed class MongoKeyValueStoreTests : IDisposable
{
    private readonly MongoDbRunner _runner;
    private readonly MongoKeyValueStore _store;

    public MongoKeyValueStoreTests()
    {
        _runner = MongoDbRunner.Start();
        var client = new MongoClient(_runner.ConnectionString);
        _store = new MongoKeyValueStore(client, $"testdb_{Guid.NewGuid():N}", $"kv_{Guid.NewGuid():N}");
    }

    [Fact]
    public async Task SetAndGet_ShouldRoundTrip()
    {
        var key = NewKey();

        var set = await _store.SetAsync(key, Bytes("value-1"), null, SetCondition.None);
        var value = await _store.GetAsync(key);

        Assert.True(set);
        Assert.Equal("value-1", Text(value));
    }

    [Fact]
    public async Task Set_Nx_ShouldNotOverwriteExistingValue()
    {
        var key = NewKey();
        await _store.SetAsync(key, Bytes("initial"), null, SetCondition.None);

        var result = await _store.SetAsync(key, Bytes("replacement"), null, SetCondition.Nx);
        var value = await _store.GetAsync(key);

        Assert.False(result);
        Assert.Equal("initial", Text(value));
    }

    [Fact]
    public async Task Set_Xx_ShouldOnlySucceedForExistingKey()
    {
        var key = NewKey();

        var missingResult = await _store.SetAsync(key, Bytes("value"), null, SetCondition.Xx);
        await _store.SetAsync(key, Bytes("original"), null, SetCondition.None);
        var existingResult = await _store.SetAsync(key, Bytes("updated"), null, SetCondition.Xx);
        var value = await _store.GetAsync(key);

        Assert.False(missingResult);
        Assert.True(existingResult);
        Assert.Equal("updated", Text(value));
    }

    [Fact]
    public async Task GetMany_ShouldRespectInputOrderAndMissingKeys()
    {
        var keyA = NewKey();
        var keyB = NewKey();
        var missing = NewKey();

        await _store.SetAsync(keyA, Bytes("A"), null, SetCondition.None);
        await _store.SetAsync(keyB, Bytes("B"), null, SetCondition.None);

        var values = await _store.GetManyAsync(new[] { keyB, missing, keyA });

        Assert.Equal(3, values.Length);
        Assert.Equal("B", Text(values[0]));
        Assert.Null(values[1]);
        Assert.Equal("A", Text(values[2]));
    }

    [Fact]
    public async Task IncrBy_ShouldCreateAndIncrementNumericValue()
    {
        var key = NewKey();

        var first = await _store.IncrByAsync(key, 2);
        var second = await _store.IncrByAsync(key, 5);
        var value = await _store.GetAsync(key);

        Assert.Equal(2, first);
        Assert.Equal(7, second);
        Assert.Equal("7", Text(value));
    }

    [Fact]
    public async Task IncrBy_ShouldReturnNullForNonNumericValue()
    {
        var key = NewKey();
        await _store.SetAsync(key, Bytes("not-a-number"), null, SetCondition.None);

        var result = await _store.IncrByAsync(key, 1);

        Assert.Null(result);
    }

    [Fact]
    public async Task Expire_ShouldMakeKeyUnavailableAfterTimeout()
    {
        var key = NewKey();
        await _store.SetAsync(key, Bytes("temp"), null, SetCondition.None);

        var expireResult = await _store.ExpireAsync(key, TimeSpan.FromMilliseconds(150));
        await Task.Delay(250);

        var value = await _store.GetAsync(key);
        var ttl = await _store.PttlAsync(key);

        Assert.True(expireResult);
        Assert.Null(value);
        Assert.Equal(-2, ttl);
    }

    [Fact]
    public async Task Delete_ShouldRemoveMultipleKeys()
    {
        var key1 = NewKey();
        var key2 = NewKey();
        await _store.SetAsync(key1, Bytes("v1"), null, SetCondition.None);
        await _store.SetAsync(key2, Bytes("v2"), null, SetCondition.None);

        var deleted = await _store.DeleteAsync(new[] { key1, key2 });

        Assert.Equal(2, deleted);
        Assert.Null(await _store.GetAsync(key1));
        Assert.Null(await _store.GetAsync(key2));
    }

    public void Dispose()
    {
        _runner.Dispose();
    }

    private static string NewKey() => $"k_{Guid.NewGuid():N}";

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    private static string? Text(byte[]? value) => value is null ? null : Encoding.UTF8.GetString(value);
}