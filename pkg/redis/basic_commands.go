package redis

import (
	"fmt"
	"strconv"
	"time"
)

// ======================== Basic Redis Commands ========================

// Set implements Redis SET command
func (r *RedisWrapper) Set(args []string) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'set' command\r\n"
	}

	key := args[1]
	value := args[2]

	r.mu.Lock()
	defer r.mu.Unlock()

	// Set the key-value pair
	err := r.engine.Put(key, value)
	if err != nil {
		return "-ERR " + err.Error() + "\r\n"
	}

	// Remove any existing TTL
	expireKey := r.getExpireKey(key)
	r.engine.Delete(expireKey) // Don't care if it doesn't exist

	return "+OK\r\n"
}

// Get implements Redis GET command
func (r *RedisWrapper) Get(args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'get' command\r\n"
	}

	key := args[1]

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check if key is expired and clean if needed
	if r.expireCleanKey(key) {
		return "$-1\r\n" // Key not found/expired
	}

	value, err := r.getEngineValue(key)
	if err != nil || value == nil {
		return "$-1\r\n" // Key not found
	}

	return fmt.Sprintf("$%d\r\n%s\r\n", len(*value), *value)
}

// Del implements Redis DEL command
func (r *RedisWrapper) Del(args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'del' command\r\n"
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	delCount := 0
	for i := 1; i < len(args); i++ {
		key := args[i]

		// Check if key exists
		value, err := r.getEngineValue(key)
		if err != nil || value == nil {
			continue
		}

		// If it's a hash value, clean up all fields
		if r.isValueHash(*value) {
			fields := r.getFieldsFromHashValue(*value)
			for _, field := range fields {
				fieldKey := r.getHashFieldKey(key, field)
				r.engine.Delete(fieldKey)
			}
		}

		// Delete the key itself
		r.engine.Delete(key)

		// Delete any TTL
		expireKey := r.getExpireKey(key)
		r.engine.Delete(expireKey)

		delCount++
	}

	return fmt.Sprintf(":%d\r\n", delCount)
}

// Incr implements Redis INCR command
func (r *RedisWrapper) Incr(args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'incr' command\r\n"
	}

	key := args[1]

	r.mu.Lock()
	defer r.mu.Unlock()

	// Get current value
	value, err := r.getEngineValue(key)
	var currentVal int64 = 0

	if err == nil && value != nil {
		currentVal, err = strconv.ParseInt(*value, 10, 64)
		if err != nil {
			return "-ERR value is not an integer or out of range\r\n"
		}
	}

	// Increment
	newVal := currentVal + 1
	newValStr := strconv.FormatInt(newVal, 10)

	// Store new value
	err = r.engine.Put(key, newValStr)
	if err != nil {
		return "-ERR " + err.Error() + "\r\n"
	}

	return fmt.Sprintf(":%d\r\n", newVal)
}

// Decr implements Redis DECR command
func (r *RedisWrapper) Decr(args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'decr' command\r\n"
	}

	key := args[1]

	r.mu.Lock()
	defer r.mu.Unlock()

	// Get current value
	value, err := r.getEngineValue(key)
	var currentVal int64 = 0

	if err == nil && value != nil {
		currentVal, err = strconv.ParseInt(*value, 10, 64)
		if err != nil {
			return "-ERR value is not an integer or out of range\r\n"
		}
	}

	// Decrement
	newVal := currentVal - 1
	newValStr := strconv.FormatInt(newVal, 10)

	// Store new value
	err = r.engine.Put(key, newValStr)
	if err != nil {
		return "-ERR " + err.Error() + "\r\n"
	}

	return fmt.Sprintf(":%d\r\n", newVal)
}

// Expire implements Redis EXPIRE command
func (r *RedisWrapper) Expire(args []string) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'expire' command\r\n"
	}

	key := args[1]
	secondsStr := args[2]

	seconds, err := strconv.ParseInt(secondsStr, 10, 64)
	if err != nil {
		return "-ERR invalid expire time in 'expire' command\r\n"
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if key exists
	value, err := r.getEngineValue(key)
	if err != nil || value == nil {
		return ":0\r\n" // Key doesn't exist
	}

	// Set expiration time
	expireKey := r.getExpireKey(key)
	expireTime := r.getExpireTime(seconds)

	err = r.engine.Put(expireKey, expireTime)
	if err != nil {
		return "-ERR " + err.Error() + "\r\n"
	}

	return ":1\r\n"
}

// TTL implements Redis TTL command
func (r *RedisWrapper) TTL(args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'ttl' command\r\n"
	}

	key := args[1]

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check if key exists
	value, err := r.getEngineValue(key)
	if err != nil || value == nil {
		return ":-2\r\n" // Key doesn't exist
	}

	// Check if TTL is set
	expireKey := r.getExpireKey(key)
	expireValue, err := r.getEngineValue(expireKey)
	if err != nil || expireValue == nil {
		return ":-1\r\n" // Key exists but no TTL set
	}

	expireTime, err := strconv.ParseInt(*expireValue, 10, 64)
	if err != nil {
		return ":-1\r\n" // Invalid TTL format
	}

	now := time.Now().Unix()

	if now > expireTime {
		return ":-2\r\n" // Key has expired (but not cleaned yet)
	}

	ttlSeconds := expireTime - now
	return fmt.Sprintf(":%d\r\n", ttlSeconds)
}
