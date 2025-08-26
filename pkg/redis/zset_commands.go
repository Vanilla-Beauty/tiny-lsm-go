package redis

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// ======================== ZSet Commands ========================

// ZSetElement represents an element in a sorted set
type ZSetElement struct {
	Member string
	Score  float64
}

// expireCleanZSet checks and cleans expired sorted set data
func (r *RedisWrapper) expireCleanZSet(key string) bool {
	expireKey := r.getExpireKey(key)
	expireValue, err := r.getEngineValue(expireKey)
	if err != nil || expireValue == nil {
		return false
	}

	if r.isExpired(*expireValue) {
		// Clean up sorted set and its members
		prefix := r.getSortedSetPrefix(key)
		memberKeys := r.scanPrefix(prefix)

		// Delete all member keys (both SCORE_ and ELEM_ keys)
		for _, memberKey := range memberKeys {
			r.engine.Delete(memberKey)
		}

		// Delete set metadata key and expire key
		r.engine.Delete(key)
		r.engine.Delete(expireKey)

		return true
	}

	return false
}

// extractScoreFromKey extracts score from a SCORE_ key
func (r *RedisWrapper) extractScoreFromKey(scoreKey string) string {
	scorePrefix := "_SCORE_"
	pos := strings.Index(scoreKey, scorePrefix)
	if pos == -1 {
		return ""
	}
	return scoreKey[pos+len(scorePrefix):]
}

// ZAdd implements Redis ZADD command
func (r *RedisWrapper) ZAdd(args []string) string {
	if len(args) < 4 || (len(args)-2)%2 != 0 {
		return "-ERR wrong number of arguments for 'zadd' command\r\n"
	}

	key := args[1]

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if sorted set is expired
	if r.expireCleanZSet(key) {
		// ZSet was expired and cleaned
	}

	// Initialize sorted set if doesn't exist
	if _, err := r.getEngineValue(key); err != nil {
		// Create sorted set metadata
		prefix := r.getSortedSetPrefix(key)
		err := r.engine.Put(key, prefix)
		if err != nil {
			return "-ERR " + err.Error() + "\r\n"
		}
	}

	addedCount := 0

	// Process score-member pairs
	for i := 2; i < len(args); i += 2 {
		scoreStr := args[i]
		member := args[i+1]

		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			return "-ERR value is not a valid float\r\n"
		}

		// Check if member already exists
		elemKey := r.getSortedSetElemKey(key, member)
		oldScoreValue, err := r.getEngineValue(elemKey)

		if err == nil && oldScoreValue != nil {
			// Member exists, remove old score entry
			oldScore, _ := strconv.ParseFloat(*oldScoreValue, 64)
			oldScoreKey := r.getSortedSetScoreKey(key, fmt.Sprintf("%.0f", oldScore))
			r.engine.Delete(oldScoreKey)
		} else {
			// New member
			addedCount++
		}

		// Add new score entries
		scoreKey := r.getSortedSetScoreKey(key, fmt.Sprintf("%.0f", score))
		err = r.engine.Put(scoreKey, member)
		if err != nil {
			return "-ERR " + err.Error() + "\r\n"
		}

		err = r.engine.Put(elemKey, scoreStr)
		if err != nil {
			return "-ERR " + err.Error() + "\r\n"
		}
	}

	return fmt.Sprintf(":%d\r\n", addedCount)
}

// ZRem implements Redis ZREM command
func (r *RedisWrapper) ZRem(args []string) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'zrem' command\r\n"
	}

	key := args[1]

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if sorted set is expired
	if r.expireCleanZSet(key) {
		return ":0\r\n" // ZSet expired, nothing to remove
	}

	removedCount := 0

	// Remove each member
	for i := 2; i < len(args); i++ {
		member := args[i]
		elemKey := r.getSortedSetElemKey(key, member)

		// Get member's score
		scoreValue, err := r.getEngineValue(elemKey)
		if err == nil && scoreValue != nil {
			// Member exists, remove both score and element entries
			score, _ := strconv.ParseFloat(*scoreValue, 64)
			scoreKey := r.getSortedSetScoreKey(key, fmt.Sprintf("%.0f", score))

			r.engine.Delete(elemKey)
			r.engine.Delete(scoreKey)
			removedCount++
		}
	}

	return fmt.Sprintf(":%d\r\n", removedCount)
}

// ZRange implements Redis ZRANGE command
func (r *RedisWrapper) ZRange(args []string) string {
	if len(args) < 4 {
		return "-ERR wrong number of arguments for 'zrange' command\r\n"
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

	// Check if sorted set is expired
	if r.expireCleanZSet(key) {
		return "*0\r\n" // ZSet expired
	}

	// Get all elements sorted by score
	scorePrefix := r.getSortedSetScorePrefix(key)
	scoreKeys := r.scanPrefix(scorePrefix)

	if len(scoreKeys) == 0 {
		return "*0\r\n" // Empty sorted set
	}

	// Sort score keys (they should already be sorted due to key format)
	sort.Strings(scoreKeys)

	// Extract members in score order
	var elements []string
	for _, scoreKey := range scoreKeys {
		member, err := r.getEngineValue(scoreKey)
		if err == nil && member != nil {
			elements = append(elements, *member)
		}
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

// ZCard implements Redis ZCARD command
func (r *RedisWrapper) ZCard(args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'zcard' command\r\n"
	}

	key := args[1]

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check if sorted set is expired
	if r.expireCleanZSet(key) {
		return ":0\r\n" // ZSet expired
	}

	// Count elements using score prefix
	scorePrefix := r.getSortedSetScorePrefix(key)
	scoreKeys := r.scanPrefix(scorePrefix)

	return fmt.Sprintf(":%d\r\n", len(scoreKeys))
}

// ZScore implements Redis ZSCORE command
func (r *RedisWrapper) ZScore(args []string) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'zscore' command\r\n"
	}

	key := args[1]
	member := args[2]

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check if sorted set is expired
	if r.expireCleanZSet(key) {
		return "$-1\r\n" // ZSet expired
	}

	// Get member's score
	elemKey := r.getSortedSetElemKey(key, member)
	scoreValue, err := r.getEngineValue(elemKey)
	if err != nil || scoreValue == nil {
		return "$-1\r\n" // Member not found
	}

	return fmt.Sprintf("$%d\r\n%s\r\n", len(*scoreValue), *scoreValue)
}

// ZIncrBy implements Redis ZINCRBY command
func (r *RedisWrapper) ZIncrBy(args []string) string {
	if len(args) < 4 {
		return "-ERR wrong number of arguments for 'zincrby' command\r\n"
	}

	key := args[1]
	incrementStr := args[2]
	member := args[3]

	increment, err := strconv.ParseFloat(incrementStr, 64)
	if err != nil {
		return "-ERR value is not a valid float\r\n"
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if sorted set is expired
	if r.expireCleanZSet(key) {
		// ZSet was expired and cleaned
	}

	// Get current score
	elemKey := r.getSortedSetElemKey(key, member)
	var currentScore float64 = 0

	scoreValue, err := r.getEngineValue(elemKey)
	if err == nil && scoreValue != nil {
		currentScore, _ = strconv.ParseFloat(*scoreValue, 64)

		// Remove old score entry
		oldScoreKey := r.getSortedSetScoreKey(key, fmt.Sprintf("%.0f", currentScore))
		r.engine.Delete(oldScoreKey)
	}

	// Calculate new score
	newScore := currentScore + increment
	newScoreStr := fmt.Sprintf("%.0f", newScore)

	// Add new entries
	scoreKey := r.getSortedSetScoreKey(key, newScoreStr)
	err = r.engine.Put(scoreKey, member)
	if err != nil {
		return "-ERR " + err.Error() + "\r\n"
	}

	err = r.engine.Put(elemKey, newScoreStr)
	if err != nil {
		return "-ERR " + err.Error() + "\r\n"
	}

	// Ensure sorted set metadata exists
	if _, err := r.getEngineValue(key); err != nil {
		prefix := r.getSortedSetPrefix(key)
		r.engine.Put(key, prefix)
	}

	return fmt.Sprintf(":%s\r\n", newScoreStr)
}

// ZRank implements Redis ZRANK command
func (r *RedisWrapper) ZRank(args []string) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'zrank' command\r\n"
	}

	key := args[1]
	member := args[2]

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check if sorted set is expired
	if r.expireCleanZSet(key) {
		return "$-1\r\n" // ZSet expired
	}

	// Get member's score
	elemKey := r.getSortedSetElemKey(key, member)
	scoreValue, err := r.getEngineValue(elemKey)
	if err != nil || scoreValue == nil {
		return "$-1\r\n" // Member not found
	}

	// Get all elements sorted by score to find rank
	scorePrefix := r.getSortedSetScorePrefix(key)
	scoreKeys := r.scanPrefix(scorePrefix)
	sort.Strings(scoreKeys) // Ensure sorted order

	// Find the member's rank
	for rank, scoreKey := range scoreKeys {
		memberValue, err := r.getEngineValue(scoreKey)
		if err == nil && memberValue != nil && *memberValue == member {
			return fmt.Sprintf(":%d\r\n", rank)
		}
	}

	return "$-1\r\n" // Member not found (shouldn't happen)
}
