# Lab 6.5 链表
最后我们来实现`Redis`中的链表。之前的`Lab`中, 作者对设计方案都进行了详细的介绍, 也许你觉得这限制了你自己的设计, 因此这一小节作者打算不给你任何提示, 你需要自己设计并实现一个链表, 并通过对应的单元测试。

# 1 代码实现
你需要修改的代码文件包括:
- `src/redis_wrapper/redis_wrapper.cpp`
- `include/redis_wrapper/redis_wrapper.h` (Optional)

> 下面的接口中, 你仍然需要进行`TTL`超时时间的判断, 同时你可能需要更新之前的`redis_ttl`和`redis_expire`以兼容`List`的`TTL`机制。

```go
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
```

**Hint**
如果你对`Redis`的`RESP`协议不熟悉, 首先推荐你看一看附录[RESP](../appendix/RESP.md), 然后直接问AI也是可以的

# 2 测试
现在你应该可以通过所有的单元测试:
```bash
✗  go test ./pkg/redis/
```

