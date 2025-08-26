package redis

import (
	"fmt"
	"strconv"
	"strings"
)

// ======================== Set Commands ========================

// expireCleanSet checks and cleans expired set data
func (r *RedisWrapper) expireCleanSet(key string) bool {
	expireKey := r.getExpireKey(key)
	expireValue, err := r.getEngineValue(expireKey)
	if err != nil || expireValue == nil {
		return false
	}
	
	if r.isExpired(*expireValue) {
		// Clean up set and its members
		prefix := r.getSetPrefix(key)
		memberKeys := r.scanPrefix(prefix)
		
		// Delete all member keys
		for _, memberKey := range memberKeys {
			r.engine.Delete(memberKey)
		}
		
		// Delete set size key and expire key
		r.engine.Delete(key)
		r.engine.Delete(expireKey)
		
		return true
	}
	
	return false
}

// SAdd implements Redis SADD command
func (r *RedisWrapper) SAdd(args []string) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'sadd' command\r\n"
	}
	
	key := args[1]
	
	r.mu.Lock()
	defer r.mu.Unlock()
	
	// Check if set is expired
	if r.expireCleanSet(key) {
		// Set was expired and cleaned
	}
	
	// Get current set size
	sizeValue, err := r.getEngineValue(key)
	var currentSize int = 0
	if err == nil && sizeValue != nil {
		currentSize, _ = strconv.Atoi(*sizeValue)
	}
	
	addedCount := 0
	
	// Add each member
	for i := 2; i < len(args); i++ {
		member := args[i]
		memberKey := r.getSetMemberKey(key, member)
		
		// Check if member already exists
		existing, err := r.getEngineValue(memberKey)
		if err != nil || existing == nil {
			// Member doesn't exist, add it
			err := r.engine.Put(memberKey, "1")
			if err != nil {
				return "-ERR " + err.Error() + "\r\n"
			}
			addedCount++
		}
	}
	
	// Update set size
	newSize := currentSize + addedCount
	err = r.engine.Put(key, strconv.Itoa(newSize))
	if err != nil {
		return "-ERR " + err.Error() + "\r\n"
	}
	
	return fmt.Sprintf(":%d\r\n", addedCount)
}

// SRem implements Redis SREM command
func (r *RedisWrapper) SRem(args []string) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'srem' command\r\n"
	}
	
	key := args[1]
	
	r.mu.Lock()
	defer r.mu.Unlock()
	
	// Check if set is expired
	if r.expireCleanSet(key) {
		return ":0\r\n" // Set expired, nothing to remove
	}
	
	// Get current set size
	sizeValue, err := r.getEngineValue(key)
	if err != nil || sizeValue == nil {
		return ":0\r\n" // Set doesn't exist
	}
	
	currentSize, err := strconv.Atoi(*sizeValue)
	if err != nil {
		currentSize = 0
	}
	
	removedCount := 0
	
	// Remove each member
	for i := 2; i < len(args); i++ {
		member := args[i]
		memberKey := r.getSetMemberKey(key, member)
		
		// Check if member exists
		existing, err := r.getEngineValue(memberKey)
		if err == nil && existing != nil {
			// Member exists, remove it
			r.engine.Delete(memberKey)
			removedCount++
		}
	}
	
	// Update set size
	newSize := currentSize - removedCount
	if newSize <= 0 {
		// Set becomes empty, delete it
		r.engine.Delete(key)
	} else {
		err = r.engine.Put(key, strconv.Itoa(newSize))
		if err != nil {
			return "-ERR " + err.Error() + "\r\n"
		}
	}
	
	return fmt.Sprintf(":%d\r\n", removedCount)
}

// SIsMember implements Redis SISMEMBER command
func (r *RedisWrapper) SIsMember(args []string) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'sismember' command\r\n"
	}
	
	key := args[1]
	member := args[2]
	
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	// Check if set is expired
	if r.expireCleanSet(key) {
		return ":0\r\n" // Set expired
	}
	
	// Check if member exists
	memberKey := r.getSetMemberKey(key, member)
	existing, err := r.getEngineValue(memberKey)
	if err != nil || existing == nil {
		return ":0\r\n" // Member doesn't exist
	}
	
	return ":1\r\n" // Member exists
}

// SCard implements Redis SCARD command
func (r *RedisWrapper) SCard(args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'scard' command\r\n"
	}
	
	key := args[1]
	
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	// Check if set is expired
	if r.expireCleanSet(key) {
		return ":0\r\n" // Set expired
	}
	
	// Get set size
	sizeValue, err := r.getEngineValue(key)
	if err != nil || sizeValue == nil {
		return ":0\r\n" // Set doesn't exist
	}
	
	return fmt.Sprintf(":%s\r\n", *sizeValue)
}

// SMembers implements Redis SMEMBERS command
func (r *RedisWrapper) SMembers(args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'smembers' command\r\n"
	}
	
	key := args[1]
	
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	// Check if set is expired
	if r.expireCleanSet(key) {
		return "*0\r\n" // Set expired
	}
	
	// Get set size to check if set exists
	sizeValue, err := r.getEngineValue(key)
	if err != nil || sizeValue == nil {
		return "*0\r\n" // Set doesn't exist
	}
	
	// Get all members using prefix scan
	prefix := r.getSetPrefix(key)
	memberKeys := r.scanPrefix(prefix)
	
	// Extract member names from keys
	var members []string
	for _, memberKey := range memberKeys {
		if strings.HasPrefix(memberKey, prefix) {
			member := memberKey[len(prefix):]
			members = append(members, member)
		}
	}
	
	// Format response
	result := fmt.Sprintf("*%d\r\n", len(members))
	for _, member := range members {
		result += fmt.Sprintf("$%d\r\n%s\r\n", len(member), member)
	}
	
	return result
}
