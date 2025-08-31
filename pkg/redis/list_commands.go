package redis

// ======================== List Commands ========================

// expireCleanList checks and cleans expired list data
func (r *RedisWrapper) expireCleanList(key string) bool {
	// TODO: Lab 6.5

	return false
}

// LPush implements Redis LPUSH command
func (r *RedisWrapper) LPush(args []string) string {
	// TODO: Lab 6.5
	return ":len_after_push\r\n"
}

// RPush implements Redis RPUSH command
func (r *RedisWrapper) RPush(args []string) string {
	// TODO: Lab 6.5
	return ":len_after_push\r\n"
}

// LPop implements Redis LPOP command
func (r *RedisWrapper) LPop(args []string) string {
	// TODO: Lab 6.5
	return ":poped_elem\r\n"
}

// RPop implements Redis RPOP command
func (r *RedisWrapper) RPop(args []string) string {
	// TODO: Lab 6.5
	return ":poped_elem\r\n"
}

// LLen implements Redis LLEN command
func (r *RedisWrapper) LLen(args []string) string {
	// TODO: Lab 6.5

	return ":len\r\n"
}

// LRange implements Redis LRANGE command
func (r *RedisWrapper) LRange(args []string) string {
	// TODO: Lab 6.5

	return ""
}
