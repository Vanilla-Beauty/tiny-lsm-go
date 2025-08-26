package redis

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"tiny-lsm-go/pkg/config"
	"tiny-lsm-go/pkg/lsm"
)

// RedisWrapper provides Redis-compatible interface on top of LSM engine
type RedisWrapper struct {
	engine *lsm.Engine
	config *config.Config
	mu     sync.RWMutex
}

// NewRedisWrapper creates a new Redis wrapper instance
func NewRedisWrapper(dbPath string) (*RedisWrapper, error) {
	cfg := config.GetGlobalConfig()
	lsmEngine, err := lsm.NewEngine(cfg, dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create LSM engine: %w", err)
	}

	return &RedisWrapper{
		engine: lsmEngine,
		config: cfg,
	}, nil
}

// Close closes the Redis wrapper and underlying LSM engine
func (r *RedisWrapper) Close() error {
	return r.engine.Close()
}

// FlushAll flushes all memtables to disk
func (r *RedisWrapper) FlushAll() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.engine.Flush()
}

// Helper functions for key formatting

// getExpireKey returns the TTL key for a given key
func (r *RedisWrapper) getExpireKey(key string) string {
	return r.config.GetRedisExpireHeader() + key
}

// getHashFieldKey returns the storage key for a hash field
func (r *RedisWrapper) getHashFieldKey(key, field string) string {
	return r.config.GetRedisFieldPrefix() + key + "_" + field
}

// isValueHash checks if a value is a hash value
func (r *RedisWrapper) isValueHash(value string) bool {
	return strings.HasPrefix(value, r.config.GetRedisHashValuePrefix())
}

// getHashValueFromFields creates a hash value string from field list
func (r *RedisWrapper) getHashValueFromFields(fields []string) string {
	separator := r.config.GetRedisFieldSeparator()
	return r.config.GetRedisHashValuePrefix() + strings.Join(fields, separator)
}

// getFieldsFromHashValue extracts field list from hash value string
func (r *RedisWrapper) getFieldsFromHashValue(value string) []string {
	if value == "" {
		return nil
	}
	
	prefix := r.config.GetRedisHashValuePrefix()
	if !strings.HasPrefix(value, prefix) {
		return nil
	}
	
	fieldList := value[len(prefix):]
	if fieldList == "" {
		return nil
	}
	
	separator := r.config.GetRedisFieldSeparator()
	return strings.Split(fieldList, separator)
}

// getSortedSetScoreKey returns the score-based key for sorted set
func (r *RedisWrapper) getSortedSetScoreKey(key, score string) string {
	// Pad score to fixed width for proper sorting
	scoreLen := r.config.GetRedisSortedSetScoreLen()
	paddedScore := fmt.Sprintf("%0*s", scoreLen, score)
	return r.config.GetRedisSortedSetPrefix() + key + "_SCORE_" + paddedScore
}

// getSortedSetElemKey returns the element-based key for sorted set
func (r *RedisWrapper) getSortedSetElemKey(key, elem string) string {
	return r.config.GetRedisSortedSetPrefix() + key + "_ELEM_" + elem
}

// getSortedSetPrefix returns the prefix for sorted set keys
func (r *RedisWrapper) getSortedSetPrefix(key string) string {
	return r.config.GetRedisSortedSetPrefix() + key + "_"
}

// getSortedSetScorePrefix returns the prefix for sorted set score keys
func (r *RedisWrapper) getSortedSetScorePrefix(key string) string {
	return r.config.GetRedisSortedSetPrefix() + key + "_SCORE_"
}

// getSetMemberKey returns the member key for sets
func (r *RedisWrapper) getSetMemberKey(key, member string) string {
	return r.config.GetRedisSetPrefix() + key + "_" + member
}

// getSetPrefix returns the prefix for set keys
func (r *RedisWrapper) getSetPrefix(key string) string {
	return r.config.GetRedisSetPrefix() + key + "_"
}

// TTL and expiration utilities

// isExpired checks if a key has expired
func (r *RedisWrapper) isExpired(expireValue string) bool {
	if expireValue == "" {
		return false
	}
	
	expireTime, err := strconv.ParseInt(expireValue, 10, 64)
	if err != nil {
		return false
	}
	
	now := time.Now().Unix()
	return now > expireTime
}

// getExpireTime calculates expire time for given seconds
func (r *RedisWrapper) getExpireTime(seconds int64) string {
	now := time.Now().Unix()
	return strconv.FormatInt(now+seconds, 10)
}

// expireCleanHash checks and cleans expired hash data
func (r *RedisWrapper) expireCleanHash(key string) bool {
	expireKey := r.getExpireKey(key)
	expireValue, err := r.getEngineValue(expireKey)
	if err != nil || expireValue == nil {
		return false
	}
	
	if r.isExpired(*expireValue) {
		// Clean up all hash fields
		hashValue, err := r.getEngineValue(key)
		if err == nil && hashValue != nil {
			fields := r.getFieldsFromHashValue(*hashValue)
			for _, field := range fields {
				fieldKey := r.getHashFieldKey(key, field)
				r.engine.Delete(fieldKey)
			}
		}
		r.engine.Delete(key)
		r.engine.Delete(expireKey)
		return true
	}
	
	return false
}

// expireCleanKey checks and cleans expired simple key
func (r *RedisWrapper) expireCleanKey(key string) bool {
	expireKey := r.getExpireKey(key)
	expireValue, err := r.getEngineValue(expireKey)
	if err != nil || expireValue == nil {
		return false
	}
	
	if r.isExpired(*expireValue) {
		r.engine.Delete(key)
		r.engine.Delete(expireKey)
		return true
	}
	
	return false
}

// splitList splits a list string into elements
func (r *RedisWrapper) splitList(listValue string) []string {
	if listValue == "" {
		return nil
	}
	separator := r.config.GetRedisListSeparator()
	return strings.Split(listValue, separator)
}

// joinList joins elements into a list string
func (r *RedisWrapper) joinList(elements []string) string {
	separator := r.config.GetRedisListSeparator()
	return strings.Join(elements, separator)
}

// getEngineValue is a helper to get value from engine (handles 3-return signature)
func (r *RedisWrapper) getEngineValue(key string) (*string, error) {
	value, found, err := r.engine.Get(key)
	// fmt.Printf("DEBUG getEngineValue: key=%q, value=%q, found=%v, err=%v\n", key, value, found, err)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return &value, nil
}

// scanPrefix scans for all keys with given prefix
func (r *RedisWrapper) scanPrefix(prefix string) []string {
	iter := r.engine.NewIterator()
	defer iter.Close()
	
	var results []string
	iter.Seek(prefix)
	
	for iter.Valid() {
		key := iter.Key()
		if !strings.HasPrefix(key, prefix) {
			break
		}
		
		if !iter.IsDeleted() {
			results = append(results, key)
		}
		
		iter.Next()
	}
	
	return results
}
