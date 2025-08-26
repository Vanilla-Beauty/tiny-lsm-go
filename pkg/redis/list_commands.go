package redis

import (
	"fmt"
	"strconv"
)

// ======================== List Commands ========================

// expireCleanList checks and cleans expired list data
func (r *RedisWrapper) expireCleanList(key string) bool {
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

// LPush implements Redis LPUSH command
func (r *RedisWrapper) LPush(args []string) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'lpush' command\r\n"
	}
	
	key := args[1]
	value := args[2]
	
	r.mu.Lock()
	defer r.mu.Unlock()
	
	// Check if list is expired
	if r.expireCleanList(key) {
		// List was expired and cleaned
	}
	
	// Get current list
	listValue, err := r.getEngineValue(key)
	var listStr string
	if err == nil && listValue != nil {
		listStr = *listValue
	}
	
	// Add to front
	if listStr == "" {
		listStr = value
	} else {
		listStr = value + r.config.GetRedisListSeparator() + listStr
	}
	
	// Store updated list
	err = r.engine.Put(key, listStr)
	if err != nil {
		return "-ERR " + err.Error() + "\r\n"
	}
	
	// Return new length
	elements := r.splitList(listStr)
	return fmt.Sprintf(":%d\r\n", len(elements))
}

// RPush implements Redis RPUSH command
func (r *RedisWrapper) RPush(args []string) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'rpush' command\r\n"
	}
	
	key := args[1]
	value := args[2]
	
	r.mu.Lock()
	defer r.mu.Unlock()
	
	// Check if list is expired
	if r.expireCleanList(key) {
		// List was expired and cleaned
	}
	
	// Get current list
	listValue, err := r.getEngineValue(key)
	var listStr string
	if err == nil && listValue != nil {
		listStr = *listValue
	}
	
	// Add to back
	if listStr == "" {
		listStr = value
	} else {
		listStr = listStr + r.config.GetRedisListSeparator() + value
	}
	
	// Store updated list
	err = r.engine.Put(key, listStr)
	if err != nil {
		return "-ERR " + err.Error() + "\r\n"
	}
	
	// Return new length
	elements := r.splitList(listStr)
	return fmt.Sprintf(":%d\r\n", len(elements))
}

// LPop implements Redis LPOP command
func (r *RedisWrapper) LPop(args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'lpop' command\r\n"
	}
	
	key := args[1]
	
	r.mu.Lock()
	defer r.mu.Unlock()
	
	// Check if list is expired
	if r.expireCleanList(key) {
		return "$-1\r\n" // List expired
	}
	
	// Get current list
	listValue, err := r.getEngineValue(key)
	if err != nil || listValue == nil {
		return "$-1\r\n" // List doesn't exist
	}
	
	elements := r.splitList(*listValue)
	if len(elements) == 0 {
		return "$-1\r\n" // Empty list
	}
	
	// Get first element
	poppedValue := elements[0]
	
	// Update list
	if len(elements) == 1 {
		// List becomes empty, delete it
		r.engine.Delete(key)
	} else {
		// Remove first element
		newElements := elements[1:]
		newListStr := r.joinList(newElements)
		r.engine.Put(key, newListStr)
	}
	
	return fmt.Sprintf("$%d\r\n%s\r\n", len(poppedValue), poppedValue)
}

// RPop implements Redis RPOP command
func (r *RedisWrapper) RPop(args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'rpop' command\r\n"
	}
	
	key := args[1]
	
	r.mu.Lock()
	defer r.mu.Unlock()
	
	// Check if list is expired
	if r.expireCleanList(key) {
		return "$-1\r\n" // List expired
	}
	
	// Get current list
	listValue, err := r.getEngineValue(key)
	if err != nil || listValue == nil {
		return "$-1\r\n" // List doesn't exist
	}
	
	elements := r.splitList(*listValue)
	if len(elements) == 0 {
		return "$-1\r\n" // Empty list
	}
	
	// Get last element
	poppedValue := elements[len(elements)-1]
	
	// Update list
	if len(elements) == 1 {
		// List becomes empty, delete it
		r.engine.Delete(key)
	} else {
		// Remove last element
		newElements := elements[:len(elements)-1]
		newListStr := r.joinList(newElements)
		r.engine.Put(key, newListStr)
	}
	
	return fmt.Sprintf("$%d\r\n%s\r\n", len(poppedValue), poppedValue)
}

// LLen implements Redis LLEN command
func (r *RedisWrapper) LLen(args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'llen' command\r\n"
	}
	
	key := args[1]
	
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	// Check if list is expired
	if r.expireCleanList(key) {
		return ":0\r\n" // List expired
	}
	
	// Get current list
	listValue, err := r.getEngineValue(key)
	if err != nil || listValue == nil {
		return ":0\r\n" // List doesn't exist
	}
	
	elements := r.splitList(*listValue)
	return fmt.Sprintf(":%d\r\n", len(elements))
}

// LRange implements Redis LRANGE command
func (r *RedisWrapper) LRange(args []string) string {
	if len(args) < 4 {
		return "-ERR wrong number of arguments for 'lrange' command\r\n"
	}
	
	key := args[1]
	startStr := args[2]
	stopStr := args[3]
	
	start, err := strconv.Atoi(startStr)
	if err != nil {
		return "-ERR value is not an integer or out of range\r\n"
	}
	
	stop, err := strconv.Atoi(stopStr)
	if err != nil {
		return "-ERR value is not an integer or out of range\r\n"
	}
	
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	// Check if list is expired
	if r.expireCleanList(key) {
		return "*0\r\n" // List expired
	}
	
	// Get current list
	listValue, err := r.getEngineValue(key)
	if err != nil || listValue == nil {
		return "*0\r\n" // List doesn't exist
	}
	
	elements := r.splitList(*listValue)
	if len(elements) == 0 {
		return "*0\r\n" // Empty list
	}
	
	// Handle negative indices
	if start < 0 {
		start += len(elements)
	}
	if stop < 0 {
		stop += len(elements)
	}
	
	// Clamp to valid range
	if start < 0 {
		start = 0
	}
	if stop >= len(elements) {
		stop = len(elements) - 1
	}
	if start > stop {
		return "*0\r\n"
	}
	
	// Extract range
	rangeElements := elements[start : stop+1]
	
	// Format response
	result := fmt.Sprintf("*%d\r\n", len(rangeElements))
	for _, elem := range rangeElements {
		result += fmt.Sprintf("$%d\r\n%s\r\n", len(elem), elem)
	}
	
	return result
}
