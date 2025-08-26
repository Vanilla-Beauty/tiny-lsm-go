package redis

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"tiny-lsm-go/pkg/config"
)

// setupTestRedis creates a test Redis wrapper instance
func setupTestRedis(t *testing.T) (*RedisWrapper, string) {
	testDir := "test_redis_data_" + t.Name()
	if err := os.RemoveAll(testDir); err != nil {
		t.Logf("Warning: failed to remove test directory: %v", err)
	}
	
	// Set up test config
	cfg := config.DefaultConfig()
	config.SetGlobalConfig(cfg)
	
	redis, err := NewRedisWrapper(testDir)
	require.NoError(t, err)
	
	return redis, testDir
}

// cleanupTestRedis cleans up test resources
func cleanupTestRedis(redis *RedisWrapper, testDir string) {
	if redis != nil {
		redis.Close()
	}
	os.RemoveAll(testDir)
}

func TestRedisWrapperBasicOperations(t *testing.T) {
	redis, testDir := setupTestRedis(t)
	defer cleanupTestRedis(redis, testDir)
	
	// Test SET and GET
	setArgs := []string{"SET", "mykey", "myvalue"}
	getArgs := []string{"GET", "mykey"}
	
	result := redis.Set(setArgs)
	t.Logf("SET result: %q", result)
	assert.Equal(t, "+OK\r\n", result)
	
	result = redis.Get(getArgs)
	t.Logf("GET result: %q", result)
	assert.Equal(t, "$7\r\nmyvalue\r\n", result)
	
	// Test non-existent key
	getNonExistentArgs := []string{"GET", "nonexistent"}
	result = redis.Get(getNonExistentArgs)
	assert.Equal(t, "$-1\r\n", result)
}

func TestRedisWrapperIncrDecr(t *testing.T) {
	redis, testDir := setupTestRedis(t)
	defer cleanupTestRedis(redis, testDir)
	
	// Test INCR
	incrArgs := []string{"INCR", "counter"}
	result := redis.Incr(incrArgs)
	assert.Equal(t, ":1\r\n", result)
	
	result = redis.Incr(incrArgs)
	assert.Equal(t, ":2\r\n", result)
	
	// Test DECR
	decrArgs := []string{"DECR", "counter"}
	result = redis.Decr(decrArgs)
	assert.Equal(t, ":1\r\n", result)
	
	result = redis.Decr(decrArgs)
	assert.Equal(t, ":0\r\n", result)
	
	result = redis.Decr(decrArgs)
	assert.Equal(t, ":-1\r\n", result)
}

func TestRedisWrapperExpire(t *testing.T) {
	redis, testDir := setupTestRedis(t)
	defer cleanupTestRedis(redis, testDir)
	
	// Set a key
	setArgs := []string{"SET", "mykey", "myvalue"}
	getArgs := []string{"GET", "mykey"}
	expireArgs := []string{"EXPIRE", "mykey", "1"}
	ttlArgs := []string{"TTL", "mykey"}
	
	redis.Set(setArgs)
	
	// Key should exist
	result := redis.Get(getArgs)
	assert.Equal(t, "$7\r\nmyvalue\r\n", result)
	
	// Set expire
	result = redis.Expire(expireArgs)
	assert.Equal(t, ":1\r\n", result)
	
	// Check TTL
	result = redis.TTL(ttlArgs)
	assert.Contains(t, result, ":") // Should return positive TTL
	
	// Key should still exist immediately
	result = redis.Get(getArgs)
	assert.Equal(t, "$7\r\nmyvalue\r\n", result)
	
	// Wait for expiration
	time.Sleep(1100 * time.Millisecond)
	
	// Key should be expired now
	result = redis.Get(getArgs)
	assert.Equal(t, "$-1\r\n", result)
}

func TestRedisWrapperHashOperations(t *testing.T) {
	redis, testDir := setupTestRedis(t)
	defer cleanupTestRedis(redis, testDir)
	
	key := "myhash"
	field1 := "field1"
	value1 := "value1"
	field2 := "field2"
	value2 := "value2"
	
	// Test HSET
	hsetArgs1 := []string{"HSET", key, field1, value1}
	result := redis.HSet(hsetArgs1)
	assert.Equal(t, ":1\r\n", result)
	
	hsetArgs2 := []string{"HSET", key, field2, value2}
	result = redis.HSet(hsetArgs2)
	assert.Equal(t, ":1\r\n", result)
	
	// Test HGET
	hgetArgs1 := []string{"HGET", key, field1}
	result = redis.HGet(hgetArgs1)
	assert.Equal(t, "$6\r\nvalue1\r\n", result)
	
	hgetArgs2 := []string{"HGET", key, field2}
	result = redis.HGet(hgetArgs2)
	assert.Equal(t, "$6\r\nvalue2\r\n", result)
	
	// Test HKEYS
	hkeysArgs := []string{"HKEYS", key}
	result = redis.HKeys(hkeysArgs)
	// Should contain both fields (order may vary)
	assert.Contains(t, result, "field1")
	assert.Contains(t, result, "field2")
	assert.Contains(t, result, "*2\r\n")
	
	// Test HDEL
	hdelArgs := []string{"HDEL", key, field1}
	result = redis.HDel(hdelArgs)
	assert.Equal(t, ":1\r\n", result)
	
	// Verify field1 is deleted
	result = redis.HGet(hgetArgs1)
	assert.Equal(t, "$-1\r\n", result)
	
	// Verify field2 still exists
	result = redis.HGet(hgetArgs2)
	assert.Equal(t, "$6\r\nvalue2\r\n", result)
}

func TestRedisWrapperListOperations(t *testing.T) {
	redis, testDir := setupTestRedis(t)
	defer cleanupTestRedis(redis, testDir)
	
	key := "mylist"
	value1 := "value1"
	value2 := "value2"
	value3 := "value3"
	
	// Test LPUSH
	lpushArgs1 := []string{"LPUSH", key, value1}
	result := redis.LPush(lpushArgs1)
	assert.Equal(t, ":1\r\n", result)
	
	lpushArgs2 := []string{"LPUSH", key, value2}
	result = redis.LPush(lpushArgs2)
	assert.Equal(t, ":2\r\n", result)
	
	// Test RPUSH
	rpushArgs := []string{"RPUSH", key, value3}
	result = redis.RPush(rpushArgs)
	assert.Equal(t, ":3\r\n", result)
	
	// Test LLEN
	llenArgs := []string{"LLEN", key}
	result = redis.LLen(llenArgs)
	assert.Equal(t, ":3\r\n", result)
	
	// Test LRANGE
	lrangeArgs := []string{"LRANGE", key, "0", "-1"}
	result = redis.LRange(lrangeArgs)
	// List should be: value2, value1, value3 (value2 was pushed to front)
	assert.Contains(t, result, "value1")
	assert.Contains(t, result, "value2")
	assert.Contains(t, result, "value3")
	assert.Contains(t, result, "*3\r\n")
	
	// Test LPOP
	lpopArgs := []string{"LPOP", key}
	result = redis.LPop(lpopArgs)
	assert.Equal(t, "$6\r\nvalue2\r\n", result)
	
	// Test RPOP
	rpopArgs := []string{"RPOP", key}
	result = redis.RPop(rpopArgs)
	assert.Equal(t, "$6\r\nvalue3\r\n", result)
	
	// Test LLEN after pops
	result = redis.LLen(llenArgs)
	assert.Equal(t, ":1\r\n", result)
}

func TestRedisWrapperSetOperations(t *testing.T) {
	redis, testDir := setupTestRedis(t)
	defer cleanupTestRedis(redis, testDir)
	
	// Test SADD
	saddArgs := []string{"SADD", "myset", "member1", "member2", "member3"}
	result := redis.SAdd(saddArgs)
	assert.Equal(t, ":3\r\n", result)
	
	// Test SCARD
	scardArgs := []string{"SCARD", "myset"}
	result = redis.SCard(scardArgs)
	assert.Equal(t, ":3\r\n", result)
	
	// Test SISMEMBER
	sismemberArgs1 := []string{"SISMEMBER", "myset", "member1"}
	result = redis.SIsMember(sismemberArgs1)
	assert.Equal(t, ":1\r\n", result)
	
	sismemberArgs2 := []string{"SISMEMBER", "myset", "member4"}
	result = redis.SIsMember(sismemberArgs2)
	assert.Equal(t, ":0\r\n", result)
	
	// Test SMEMBERS
	smembersArgs := []string{"SMEMBERS", "myset"}
	result = redis.SMembers(smembersArgs)
	assert.Contains(t, result, "member1")
	assert.Contains(t, result, "member2")
	assert.Contains(t, result, "member3")
	assert.Contains(t, result, "*3\r\n")
	
	// Test SREM
	sremArgs := []string{"SREM", "myset", "member1", "member3"}
	result = redis.SRem(sremArgs)
	assert.Equal(t, ":2\r\n", result)
	
	// Test SCARD after removal
	result = redis.SCard(scardArgs)
	assert.Equal(t, ":1\r\n", result)
}

func TestRedisWrapperZSetOperations(t *testing.T) {
	redis, testDir := setupTestRedis(t)
	defer cleanupTestRedis(redis, testDir)
	
	// Test ZADD
	zaddArgs := []string{"ZADD", "myzset", "1", "one", "2", "two", "3", "three"}
	result := redis.ZAdd(zaddArgs)
	assert.Equal(t, ":3\r\n", result)
	
	// Test ZCARD
	zcardArgs := []string{"ZCARD", "myzset"}
	result = redis.ZCard(zcardArgs)
	assert.Equal(t, ":3\r\n", result)
	
	// Test ZSCORE
	zscoreArgs := []string{"ZSCORE", "myzset", "two"}
	result = redis.ZScore(zscoreArgs)
	assert.Equal(t, "$1\r\n2\r\n", result)
	
	// Test ZRANGE
	zrangeArgs := []string{"ZRANGE", "myzset", "0", "-1"}
	result = redis.ZRange(zrangeArgs)
	assert.Contains(t, result, "one")
	assert.Contains(t, result, "two")
	assert.Contains(t, result, "three")
	assert.Contains(t, result, "*3\r\n")
	
	// Test ZINCRBY
	zincrbyArgs := []string{"ZINCRBY", "myzset", "2", "two"}
	result = redis.ZIncrBy(zincrbyArgs)
	assert.Equal(t, ":4\r\n", result)
	
	// Test ZRANK
	zrankArgs := []string{"ZRANK", "myzset", "one"}
	result = redis.ZRank(zrankArgs)
	assert.Equal(t, ":0\r\n", result) // Should be first (lowest score)
	
	// Test ZREM
	zremArgs := []string{"ZREM", "myzset", "one"}
	result = redis.ZRem(zremArgs)
	assert.Equal(t, ":1\r\n", result)
	
	// Test ZCARD after removal
	result = redis.ZCard(zcardArgs)
	assert.Equal(t, ":2\r\n", result)
}

func TestRedisWrapperDel(t *testing.T) {
	redis, testDir := setupTestRedis(t)
	defer cleanupTestRedis(redis, testDir)
	
	// Set multiple keys
	redis.Set([]string{"SET", "key1", "value1"})
	redis.Set([]string{"SET", "key2", "value2"})
	redis.Set([]string{"SET", "key3", "value3"})
	
	// Delete multiple keys
	delArgs := []string{"DEL", "key1", "key2", "nonexistent"}
	result := redis.Del(delArgs)
	assert.Equal(t, ":2\r\n", result) // Only 2 keys existed
	
	// Verify keys are deleted
	result = redis.Get([]string{"GET", "key1"})
	assert.Equal(t, "$-1\r\n", result)
	
	result = redis.Get([]string{"GET", "key2"})
	assert.Equal(t, "$-1\r\n", result)
	
	// Key3 should still exist
	result = redis.Get([]string{"GET", "key3"})
	assert.Equal(t, "$6\r\nvalue3\r\n", result)
}

func TestRedisWrapperHashTTL(t *testing.T) {
	redis, testDir := setupTestRedis(t)
	defer cleanupTestRedis(redis, testDir)
	
	key := "myhash"
	field := "field1"
	value := "value1"
	
	// Set hash field
	hsetArgs := []string{"HSET", key, field, value}
	result := redis.HSet(hsetArgs)
	assert.Equal(t, ":1\r\n", result)
	
	// Set TTL
	expireArgs := []string{"EXPIRE", key, "1"}
	result = redis.Expire(expireArgs)
	assert.Equal(t, ":1\r\n", result)
	
	// Hash should exist immediately
	hgetArgs := []string{"HGET", key, field}
	result = redis.HGet(hgetArgs)
	assert.Equal(t, "$6\r\nvalue1\r\n", result)
	
	// Wait for expiration
	time.Sleep(1100 * time.Millisecond)
	
	// Hash should be expired
	result = redis.HGet(hgetArgs)
	assert.Equal(t, "$-1\r\n", result)
}

func TestRedisWrapperErrorHandling(t *testing.T) {
	redis, testDir := setupTestRedis(t)
	defer cleanupTestRedis(redis, testDir)
	
	// Test wrong number of arguments
	result := redis.Set([]string{"SET", "key"})
	assert.Contains(t, result, "ERR wrong number of arguments")
	
	result = redis.Get([]string{"GET"})
	assert.Contains(t, result, "ERR wrong number of arguments")
	
	result = redis.HSet([]string{"HSET", "key", "field"})
	assert.Contains(t, result, "ERR wrong number of arguments")
	
	// Test INCR on non-integer value
	redis.Set([]string{"SET", "nonint", "notanumber"})
	result = redis.Incr([]string{"INCR", "nonint"})
	assert.Contains(t, result, "ERR value is not an integer")
}

func TestRedisWrapperFlush(t *testing.T) {
	redis, testDir := setupTestRedis(t)
	defer cleanupTestRedis(redis, testDir)
	
	// Add some data
	redis.Set([]string{"SET", "key1", "value1"})
	redis.HSet([]string{"HSET", "hash1", "field1", "value1"})
	redis.LPush([]string{"LPUSH", "list1", "item1"})
	
	// Verify data exists
	result := redis.Get([]string{"GET", "key1"})
	assert.Equal(t, "$6\r\nvalue1\r\n", result)
	
	// Test FlushAll
	err := redis.FlushAll()
	assert.NoError(t, err)
	
	// Data should still be accessible after flush
	result = redis.Get([]string{"GET", "key1"})
	assert.Equal(t, "$6\r\nvalue1\r\n", result)
}
