# Lab 6.3 无序集合
# Redis Set 数据结构设计与实现

# 1 Redis设计思路

Redis的集合(Set)功能通过以下方式实现：
1. 使用一个元数据键存储集合的大小信息
2. 为每个集合成员创建一个独立的键值对进行存储
3. 通过组合键命名策略来组织数据

与`Hash`不同，`Set`只需要记录成员是否存在，不需要存储值，因此每个成员只需要一个键来标识其存在性。这种设计利用了KV存储的键存在性来表示集合成员关系。

# 2 具体实现方式

#### 3.2.1 数据存储结构

Set的存储结构包括三个部分：
1. 集合大小键：存储集合中成员的数量
2. 成员键：每个成员对应一个键，值固定为"1"表示存在
3. 过期键：存储集合的过期时间信息

```bash
// 集合大小键: 存储集合成员数量
key -> "3"

// 每个成员对应一个键，值为"1"表示存在
"__set__:key_member1" -> "1"
"__set__:key_member2" -> "1"
"__set__:key_member3" -> "1"

// 过期键（如果设置了过期时间）
"__expire__:key" -> "timestamp"
```

这种设计充分利用了底层KV存储的特性，通过合理的键命名策略和数据组织方式，实现了Redis集合数据结构的完整功能。与Hash不同，Set只需要记录成员的存在性，因此实现相对简单，每个成员只需一个键值对即可表示。

#### 3.2.2 SADD 命令实现

`SADD`命令用于向集合中添加一个或多个成员，实现步骤如下：
1. 检查集合是否过期，如果过期则清理
2. 获取当前集合大小
3. 对于每个要添加的成员：
   - 检查成员是否已存在
   - 如果不存在，则创建成员键并设置值为"1"
   - 更新添加计数
4. 更新集合大小键中的成员数量

#### 3.2.3 SREM 命令实现

`SREM`命令用于从集合中移除一个或多个成员，实现步骤如下：
1. 检查集合是否过期
2. 获取当前集合大小
3. 对于每个要移除的成员：
   - 检查成员是否存在
   - 如果存在，则删除对应的成员键
   - 更新移除计数
4. 更新集合大小键中的成员数量，如果集合变为空则删除集合

#### 3.2.4 SISMEMBER 命令实现

SISMEMBER命令用于检查成员是否存在于集合中，实现步骤如下：
1. 检查集合是否过期
2. 构造成员键(`__set__:key_member`)
3. 检查成员键是否存在，返回结果

#### 3.2.5 SCARD 命令实现

SCARD命令用于获取集合中成员的数量，实现步骤如下：
1. 检查集合是否过期
2. 获取集合大小键中的值
3. 返回集合大小

#### 3.2.6 SMEMBERS 命令实现

SMEMBERS命令用于获取集合中的所有成员，实现步骤如下：
1. 检查集合是否过期
2. 通过前缀扫描获取所有成员键
3. 从成员键中提取成员名
4. 返回所有成员名列表

# 3 代码实现
你需要修改的代码文件包括:
- `pkg/redis/set_commands.go`

## 3.1 关键辅助函数

`Set`功能实现中使用了以下关键辅助函数：
```go
// pkg/redis/set_commands.go
// expireCleanSet checks and cleans expired set data
func (r *RedisWrapper) expireCleanSet(key string) bool {
	// TODO: Lab 6.3

	return false
}
```
该函数和之前一样, 用于检查并清理过期集合数据。

同样地, 对于`key`的拼接等操作, `pkg/redis/redis_wrapper.go`中提供一些辅助函数:
- `func (r *RedisWrapper) getSetMemberKey(key, member string) string `: 获取`Set`类型的成员键
- `func (r *RedisWrapper) getSetPrefix(key string) string`: 获取`Set`类型的前缀

## 3.2 核心命令实现
> 下面的接口中, 你仍然需要进行`TTL`超时时间的判断, 同时你可能需要更新之前的`redis_ttl`和`redis_expire`以兼容`Set`的`TTL`机制。

### 3.2.1 sadd
```go
func (r *RedisWrapper) SAdd(args []string) string {
	// TODO: Lab 6.3
	addedCount := 0
	return fmt.Sprintf(":%d\r\n", addedCount)
} 
```
需要注意的是, `sadd`是支持一次性在集合中新增多个元素的, 这里你可能需要调用批量化操作的接口提高以性能。


### 3.2.2 srem
```go
// SRem implements Redis SREM command
func (r *RedisWrapper) SRem(args []string) string {
	// TODO: Lab 6.3

	removedCount := 0
	return fmt.Sprintf(":%d\r\n", removedCount)
}
```
需要注意的是, `srem`是支持一次性从集合中删除多个元素的, 这里你可能需要调用批量化操作的接口提高以性能。

### 3.2.3 sismember
```go
// SIsMember implements Redis SISMEMBER command
func (r *RedisWrapper) SIsMember(args []string) string {
	// TODO: Lab 6.3

	return ":1\r\n" // Member exists
}
```
对于查询单个字符串是否在集合中, 你只需要按照规则拼接这个分离存储的`key`, 再从`Lsm Tree`中查询即可。

### 3.2.4 scard
```go
func (r *RedisWrapper) SCard(args []string) string {
  // TODO: Lab 6.3 
	return ":0\r\n"
}
```
这里同样, 你可以直接在代表整个集合的大`key`中, 通过`value`中的元信息直接获取到集合的元素个数。如果你没有在大`key`中存储元信息, 调用前缀查询接口也可以获取到所有元素, 然后返回元素个数即可。

### 3.2.5 smembers
```go
// SMembers implements Redis SMEMBERS command
func (r *RedisWrapper) SMembers(args []string) string {
	// TODO: Lab 6.3
	return "*0\r\n" // Set doesn't exist
}

```

`smembers`用于获取所有的元素, 这里你可以直接调用`LSM Tree`的前缀查询接口, 查询所有前缀匹配的键值对, 然后从这些键值对的`value`构造`RESP`协议的数组返回即可。

这里作者为你提供了这样一个前缀查询的模板, 你可以选择实现并利用这个模板:
```go
// pkg/redis/redis_wrapper.go
// scanPrefix scans for all keys with given prefix
func (r *RedisWrapper) scanPrefix(prefix string) []string {
	// TODO: Lab 6.3

	var results []string

	return results
}
```

# 4 测试
在完成上面的功能后, 你可以运行下面的测试:
```bash
✗  go test ./pkg/redis/
```
此时除了之前的测试外, `TestRedisWrapperSetOperations`应该也是可以通过的