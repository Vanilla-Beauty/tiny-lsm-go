package redis

import (
	"fmt"
)

// ======================== Hash Commands ========================

// HSet implements Redis HSET command
func (r *RedisWrapper) HSet(args []string) string {
	// TODO: Lab 6.2
	addedCount := 0

	return fmt.Sprintf(":%d\r\n", addedCount)
}

// HGet implements Redis HGET command
func (r *RedisWrapper) HGet(args []string) string {
	// TODO: Lab 6.2
	return "$-1\r\n" // Field not found
}

// HDel implements Redis HDEL command
func (r *RedisWrapper) HDel(args []string) string {
	// TODO: Lab 6.2
	delCount := 0
	return fmt.Sprintf(":%d\r\n", delCount)
}

// HKeys implements Redis HKEYS command
func (r *RedisWrapper) HKeys(args []string) string {
	// TODO: Lab 6.2

	return ""
}
