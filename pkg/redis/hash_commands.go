package redis

import (
	"fmt"
	"sort"
)

// ======================== Hash Commands ========================

// HSet implements Redis HSET command
func (r *RedisWrapper) HSet(args []string) string {
	if len(args) < 4 || (len(args)-1)%2 != 1 {
		return "-ERR wrong number of arguments for 'hset' command\r\n"
	}
	
	key := args[1]
	
	r.mu.Lock()
	defer r.mu.Unlock()
	
	// Check if hash is expired and clean if needed
	if r.expireCleanHash(key) {
		// Hash was expired and cleaned
	}
	
	// Get current field list
	hashValue, _ := r.getEngineValue(key)
	var fields []string
	if hashValue != nil {
		fields = r.getFieldsFromHashValue(*hashValue)
	}
	
	// Track existing fields for counting new additions
	existingFields := make(map[string]bool)
	for _, field := range fields {
		existingFields[field] = true
	}
	
	addedCount := 0
	
	// Process field-value pairs
	for i := 2; i < len(args); i += 2 {
		field := args[i]
		value := args[i+1]
		
		// Set field value
		fieldKey := r.getHashFieldKey(key, field)
		err := r.engine.Put(fieldKey, value)
		if err != nil {
			return "-ERR " + err.Error() + "\r\n"
		}
		
		// Add to field list if new
		if !existingFields[field] {
			fields = append(fields, field)
			existingFields[field] = true
			addedCount++
		}
	}
	
	// Update hash metadata
	if len(fields) > 0 {
		sort.Strings(fields) // Keep fields sorted for consistency
		hashValueStr := r.getHashValueFromFields(fields)
		err := r.engine.Put(key, hashValueStr)
		if err != nil {
			return "-ERR " + err.Error() + "\r\n"
		}
	}
	
	return fmt.Sprintf(":%d\r\n", addedCount)
}

// HGet implements Redis HGET command
func (r *RedisWrapper) HGet(args []string) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'hget' command\r\n"
	}
	
	key := args[1]
	field := args[2]
	
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	// Check if hash is expired
	if r.expireCleanHash(key) {
		return "$-1\r\n" // Hash expired
	}
	
	// Get field value
	fieldKey := r.getHashFieldKey(key, field)
	value, err := r.getEngineValue(fieldKey)
	if err != nil || value == nil {
		return "$-1\r\n" // Field not found
	}
	
	return fmt.Sprintf("$%d\r\n%s\r\n", len(*value), *value)
}

// HDel implements Redis HDEL command
func (r *RedisWrapper) HDel(args []string) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'hdel' command\r\n"
	}
	
	key := args[1]
	
	r.mu.Lock()
	defer r.mu.Unlock()
	
	// Check if hash is expired
	if r.expireCleanHash(key) {
		return ":0\r\n" // Hash expired, nothing to delete
	}
	
	// Get current field list
	hashValue, err := r.getEngineValue(key)
	if err != nil || hashValue == nil {
		return ":0\r\n" // Hash doesn't exist
	}
	
	fields := r.getFieldsFromHashValue(*hashValue)
	if len(fields) == 0 {
		return ":0\r\n" // Empty hash
	}
	
	// Track fields to remove
	fieldsToRemove := make(map[string]bool)
	for i := 2; i < len(args); i++ {
		fieldsToRemove[args[i]] = true
	}
	
	delCount := 0
	var remainingFields []string
	
	// Process each field
	for _, field := range fields {
		if fieldsToRemove[field] {
			// Delete field value
			fieldKey := r.getHashFieldKey(key, field)
			fieldValue, err := r.getEngineValue(fieldKey)
			if err == nil && fieldValue != nil {
				r.engine.Delete(fieldKey)
				delCount++
			}
		} else {
			remainingFields = append(remainingFields, field)
		}
	}
	
	// Update hash metadata
	if len(remainingFields) == 0 {
		// No fields left, delete the hash
		r.engine.Delete(key)
	} else {
		// Update field list
		sort.Strings(remainingFields)
		hashValueStr := r.getHashValueFromFields(remainingFields)
		r.engine.Put(key, hashValueStr)
	}
	
	return fmt.Sprintf(":%d\r\n", delCount)
}

// HKeys implements Redis HKEYS command
func (r *RedisWrapper) HKeys(args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'hkeys' command\r\n"
	}
	
	key := args[1]
	
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	// Check if hash is expired
	if r.expireCleanHash(key) {
		return "*0\r\n" // Hash expired
	}
	
	// Get field list
	hashValue, err := r.getEngineValue(key)
	if err != nil || hashValue == nil {
		return "*0\r\n" // Hash doesn't exist
	}
	
	fields := r.getFieldsFromHashValue(*hashValue)
	if len(fields) == 0 {
		return "*0\r\n" // Empty hash
	}
	
	// Format response
	result := fmt.Sprintf("*%d\r\n", len(fields))
	for _, field := range fields {
		result += fmt.Sprintf("$%d\r\n%s\r\n", len(field), field)
	}
	
	return result
}
