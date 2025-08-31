package redis

import (
	"fmt"
)

// ======================== Set Commands ========================

// expireCleanSet checks and cleans expired set data
func (r *RedisWrapper) expireCleanSet(key string) bool {
	// TODO: Lab 6.3

	return false
}

// SAdd implements Redis SADD command
func (r *RedisWrapper) SAdd(args []string) string {
	// TODO: Lab 6.3
	addedCount := 0
	return fmt.Sprintf(":%d\r\n", addedCount)
}

// SRem implements Redis SREM command
func (r *RedisWrapper) SRem(args []string) string {
	// TODO: Lab 6.3

	removedCount := 0
	return fmt.Sprintf(":%d\r\n", removedCount)
}

// SIsMember implements Redis SISMEMBER command
func (r *RedisWrapper) SIsMember(args []string) string {
	// TODO: Lab 6.3

	return ":1\r\n" // Member exists
}

// SCard implements Redis SCARD command
func (r *RedisWrapper) SCard(args []string) string {
	// TODO: Lab 6.3
	return ":0\r\n"
}

// SMembers implements Redis SMEMBERS command
func (r *RedisWrapper) SMembers(args []string) string {
	// TODO: Lab 6.3
	return "*0\r\n" // Set doesn't exist
}
