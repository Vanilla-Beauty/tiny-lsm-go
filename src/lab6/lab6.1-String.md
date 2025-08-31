# Lab 6.1 简单字符串
# 1 简单字符串的设计
你也许会认为, 简单字符串不就是调用我们`LSM Tree`的`get`和`put`方法吗? 其实不然, 你是否忘记了我们的`Redis`是支持对键值对进行过期时间设置的? 

那么, 由于过期时间的存在, 你的代码实现需要解决以下几个难点:

## 1 如何实现过期时间?
你也许会认为, 我们只需要在`value`或者`key`中拼接一个字段来表示过期时间即可, 但虽然是一个可行的方案, 但因为我们过期时间是支持重新设置的, 这样以来你在查询数据后需要进行一定的字符串处理流程。

另一种方案，是每个实际的键值对绑定一个表示其生命周期的额外键值对，比如你插入的键值对是 `(a, b)`, 那你可以同时插入一个键值对 `(expire_a, expire_time)`, 其中`expire_time`表示与键`a`的绑定的表示过期时间的`key`。这样，当你查询`a`时，只需要查询`expire_a`的表示过期时间的`expire_time`即可，如果过期时间小于当前时间，则删除`a`和`expire_a`，并返回`nil`。

上面两种方案是最简单且容易想到的方案, 当然你也不一定局限于作者推荐的实现方案, 可以有自己的设计

## 2 采取何种过期清理策略?
那么`key`只要存在过期时间, 你的实现策略有以下3种:
- 惰性检查: 相同的`key`在下一次被查询时, 检查是否过期, 如果过期则删除, 返回`nil`
  - 优点: 实现简单
  - 缺点: 如果这个`key`是个冷`key`(即访问频率低), 那么即时其过期很久之后, 仍然占据了内存(虽然我们的`LSM Tree`是追加写入的, 但在`Compact`时, 我们是需要移除已经完成的事务且被覆写的键值对的)
- 后台线程检查: 在后台开启一个线程, 每隔一段时间检查所有键值对, 如果过期则删除
  - 优点: 过期的`key`能较为及时地被删除
  - 缺点: 需要额外的线程, 代码组织和并发控制复杂
- 前两种结合: 惰性检查+后台线程检查

# 3 代码组织简介
这一小节我们首先对`Redis`的兼容层代码进行简要介绍, 我们的代码组织为:
```bash
redis
├── basic_commands.go
├── hash_commands.go
├── list_commands.go
├── redis_wrapper.go      # 整体封装层
├── redis_wrapper_test.go # 单元测试
├── set_commands.go
└── zset_commands.go
```

各个代码文件的作用如上所示, 这里我们主要介绍今天要修改的`redis_wrapper.cpp`和`redis_wrapper.h`文件。

首先看`redis_wrapper.go`文件:
```go
// RedisWrapper provides Redis-compatible interface on top of LSM engine
type RedisWrapper struct {
	engine *lsm.Engine
	config *config.Config
	mu     sync.RWMutex
}
```
这里的成员变量只有一把锁、配置类和一个`LSM`对象, 锁用于保护`LSM`对象, 防止并发访问。不过这个锁的只是一个可选的使用项, 如果你之前的`LSMEngine`的接口实现了对某些批量化操作的并发控制, 那么你可以直接使用`LSMEngine`的接口, 而不需要使用`RedisWrapper`的锁。

其余部分的`redis_wrapper.go`的代码都是一些整体架构上的功能函数，后续会介绍到，至于具体的单个类型的命令实现，都在同目录的`xxx_commands.go`文件中, 例如`Hash`相关命令的实现在`hash_commands.go`文件中。

# 4 代码实现
本小节我们实现字符串处理相关命令函数, 你需要修改的代码文件包括:
- `pkg/redis/basic_commands.go`
- `pkg/redis/redis_wrapper.go`

## 4.1 set
```go
// Set implements Redis SET command
func (r *RedisWrapper) Set(args []string) string {
	// TODO: Lab 6.1

	return "$-1\r\n"
}
```

这里我们不需要你支持在`set`一个`key`时就指定其过期时间, 我们的单元测试只会在`expire`中手动设置过期时间。

## 4.2 expire
```go
// pkg/redis/basic_commands.go
// Expire implements Redis EXPIRE command
func (r *RedisWrapper) Expire(args []string) string {
	// TODO: Lab 6.1

	return ":1\r\n"
}
```
该命令用于设置一个`key`的过期时间, 单位为秒。

如同之前理论部分的介绍, 你既可以选择为其额外设置一个表示过期时间的键值对, 也可以在键值对的字符串中拼接表示过期时间的部分, 亦或是其他方案。

### 4.3 ttl
```go
// pkg/redis/basic_commands.go
// TTL implements Redis TTL command
func (r *RedisWrapper) TTL(args []string) string {
	// TODO: Lab 6.1

	return ":-1\r\n" // Key exists but no TTL set
}
```
该命令是与`expire`成对的, 你在`expire`中如何设置过期时间, 就需要在`ttl`中如何获取剩余过期时间。

## 4.4 get
```go
// pkg/redis/basic_commands.g
// Get implements Redis GET command
func (r *RedisWrapper) Get(args []string) string {
	// TODO: Lab 6.1

	return "$-1\r\n" // Key not found
}
```
查询一个`key`的值, 如果不存在则返回`nil`。

你可能需要再此时判断一下`key`是否已经过期, 如果已经过期则删除该`key`。

**Hints**:
- `pkg/redis/redis_wrapper.go`中有一些辅助函数需要你实现:
  - `func (r *RedisWrapper) getEngineValue(key string) (*string, error)`函数是调用`LSM Engine`查询物理上的键值对的方法, 这里查询之后可能涉及一些判断类型是否合法、查询结果是否删除、过期等操作，因此其比`put`涉及的内容更多, 建议你将查询接口单独实现以便于复用, 当然这不是必须的
  - `func (r *RedisWrapper) isExpired(expireValue string) bool`函数用于判断一个`key`是否已经过期, 建议你实现次函数以便于后续复用
  - `pkg/redis/redis_wrapper.go`中的`func (r *RedisWrapper) getExpireTime(seconds int64) string`函数用于将一个`int64`类型的时间转换为一个`string`类型的时间, 建议你实现次函数以便于后续复用
- `pkg/redis/redis_wrapper.go`中还有一些实现好的辅助函数可能会对你有帮助:
  - `func (r *RedisWrapper) getExpireKey(key string) string`

## 4.5 incr && decr
```go
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
```
对一个值类型的`key`进行自增或自减操作, 如果不存在则新建一个值为1或-1的`key`。

如果该键值对的值不是数值类型, 则返回`error`。在`RESP`中如何表示`error`你需要自行回顾[Lab 6 Redis 兼容](./lab6-Redis.md)中的简单介绍, 或者看官方文档(甚至是问LLM)。

## 4.6 del
```go
// Del implements Redis DEL command
func (r *RedisWrapper) Del(args []string) string {
	// TODO: Lab 6.1
	delCount := 0

	return fmt.Sprintf(":%d\r\n", delCount)
}
```
删除一个`key`。

# 5 测试
完成上面的代码后, 你可以运行以下命令并通过对应的测试:
```bash
✗ go test ./pkg/redis/
```
正常情况下, 你应该能通过`TestRedisWrapperBasicOperations`, `TestRedisWrapperDel`, `TestRedisWrapperIncrDecr`, `TestRedisWrapperExpire`这几个基本的单元测试

