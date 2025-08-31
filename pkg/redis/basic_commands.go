package redis

import (
	"fmt"
)

// ======================== Basic Redis Commands ========================

// Set implements Redis SET command
func (r *RedisWrapper) Set(args []string) string {
	// TODO: Lab 6.1

	return "$-1\r\n"
}

// Get implements Redis GET command
func (r *RedisWrapper) Get(args []string) string {
	// TODO: Lab 6.1

	return "$-1\r\n" // Key not found
}

// Del implements Redis DEL command
func (r *RedisWrapper) Del(args []string) string {
	// TODO: Lab 6.1
	delCount := 0

	return fmt.Sprintf(":%d\r\n", delCount)
}

// Incr implements Redis INCR command
func (r *RedisWrapper) Incr(args []string) string {
	// TODO: Lab 6.1

	return "-ERR " + "xxx" + "\r\n"
}

// Decr implements Redis DECR command
func (r *RedisWrapper) Decr(args []string) string {
	// TODO: Lab 6.1

	return "-ERR " + "xxx" + "\r\n"
}

// Expire implements Redis EXPIRE command
func (r *RedisWrapper) Expire(args []string) string {
	// TODO: Lab 6.1

	return ":1\r\n"
}

// TTL implements Redis TTL command
func (r *RedisWrapper) TTL(args []string) string {
	// TODO: Lab 6.1

	return ":-1\r\n" // Key exists but no TTL set
}
