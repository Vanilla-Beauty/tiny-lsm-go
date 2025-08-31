# Lab 6.2 哈希表

# 1 Redis设计思路

`Redis`的哈希功能通过以下方式实现：
1. 使用一个元数据键存储哈希表的所有字段名列表
2. 为每个字段创建一个独立的键值对存储实际值
3. 通过组合键命名策略来组织数据

这里的核心思路就是将哈希结构的大`key`和其具体成员的`key/value`分离存储, 宏观的哈希结构(称为大`key`)中存储其字段的元信息, 下面结合具体例子对这种设计进行说明:

# 2 具体实现方式
## 2.1 数据存储结构
这里大`key`存储的实际`value`通过`__hash__:`进行标识, 后续是其具体字段的名字, 用`|`进行分隔:
```bash
// 哈希元数据键: 存储字段名列表
key -> "__hash__:field1|field2|field3"
```
其具体的成员的`key`也需要和前缀`__field__`连接进行标识, 设计如下
```
// 每个字段的实际值存储在独立键中
"__field__:key_field1" -> "value1"
"__field__:key_field2" -> "value2"
```

## 2.2 HSET 命令实现
这里的哈希数据结构也需要支持过期时间, 即其也有一个表示过期时间的`expire_key`与之绑定, 但这里绑定的只能是宏观的大`key`, 其存储具体成员的`key`不需要绑定过期时间。

HSET命令的实现步骤：
1. 检查哈希是否过期，如果过期则清理
2. 获取当前字段列表
3. 对于每个要设置的字段值对：
   - 创建独立的字段键(`__field__:key_field`)存储值
   - 更新字段列表（添加新字段）
4. 将更新后的字段列表以特定格式(`__hash__:field1|field2`)存储在主键中

## 2.3 HGET 命令实现
这里和普通的字符串存储一样, 只是多了一次查询而已，需要先查主键（也就是大`key`）来获取具体的字段的`key`:

HGET命令的实现步骤：
1. 检查哈希是否过期
2. 构造字段键(`__field__:key_field`)
3. 从底层存储中获取字段值

## 2.4 HDEL 命令实现

HDEL命令的实现步骤：
1. 检查哈希是否过期
2. 获取字段列表
3. 对于每个要删除的字段：
   - 删除字段键对应的值
   - 从字段列表中移除该字段
4. 更新主键中的字段列表，如果列表为空则删除整个哈希

## 2.5. HKEYS 命令实现

HKEYS命令的实现步骤：
1. 检查哈希是否过期
2. 获取存储在主键中的字段列表
3. 返回所有字段名

# 3 代码实现
你需要修改的代码文件包括:
- `pkg/redis/hash_commands.go`
- `pkg/redis/redis_wrapper.go` (Optional)

> 下面的接口中, 你仍然需要进行`TTL`超时时间的判断, 同时你可能需要更新之前的`redis_ttl`和`redis_expire`以兼容`Hash`的`TTL`机制。

## 3.1 hset
```go
// pkg/redis/hash_commands.go
// HSet implements Redis HSET command
func (r *RedisWrapper) HSet(args []string) string {
	// TODO: Lab 6.2
	addedCount := 0

	return fmt.Sprintf(":%d\r\n", addedCount)
}
```

首先, 这里涉及到对`key`是否存在即过期的判断, 需要说明的是, 如果这个`key`已经过期, 那么这次操作相当于重置了`key`, 之前的绑定的表示示过期时间已经不存在了, 需要将`expire_key`删除。

其次, `hset`允许单次的`filed`设置, 也可以批量设置多个`filed`。

**Hints**:
- 
- `pkg/redis/redis_wrapper.go`中有一些辅助函数需要你实现或了解:
  - `func (r *RedisWrapper) expireCleanHash(key string) bool`函数用于判断并清理过期的哈希结构, 建议你实现这个函数, 便于后续开发中复用次逻辑
- `pkg/redis/redis_wrapper.go`中还有一些实现好的辅助函数可能会对你有帮助:
  - `func (r *RedisWrapper) isValueHash(value string) bool`: 判断一个字符串是否是一个哈希结构
  - `func (r *RedisWrapper) getHashFieldKey(key, field string) string`: 获取一个字段的键
  - `func (r *RedisWrapper) getHashValueFromFields(fields []string) string`: 从哈希的多个字段名构建主键的`value`
  - `func (r *RedisWrapper) getFieldsFromHashValue(value string) []string`: 上一个函数的逆向操作, 从编码后的主键`value`解析出所有的字段名

## 3.2 hget
```go
// HGet implements Redis HGET command
func (r *RedisWrapper) HGet(args []string) string {
	// TODO: Lab 6.2
	return "$-1\r\n" // Field not found
}
```

类似之前的简单字符串的`get`操作, 你可能需要再此时判断一下`key`是否已经过期, 如果已经过期则删除该`key`及其`filed`(如果是分离存储的实现方案)。

## 3.3 hdel
```go
// HDel implements Redis HDEL command
func (r *RedisWrapper) HDel(args []string) string {
	// TODO: Lab 6.2
	delCount := 0
	return fmt.Sprintf(":%d\r\n", delCount)
}
```
你需要删除哈希结构的`filed`, 这里的接口是支持一次性删除多个`filed`的, 返回值也是成功的删除数量。
这里需要注意的是, 你删除了一个`filed`后, 主键的元信息也是需要更新的。

## 3.4 hkeys
```go
// HKeys implements Redis HKEYS command
func (r *RedisWrapper) HKeys(args []string) string {
	// TODO: Lab 6.2

	return ""
}
```
`_hkeys`就是返回哈希结构中所有的`filed`。

同时,这也是为什么在之前的理论介绍中 (在分离存储`filed`的实现方案中), 为什么建议你在代表整个哈希结构的大`key`的`value`中存储所有`filed`的元信息。长这样你在实现 `hkeys`的时候就会方便很多, 只需要查询单个`key`就可以了。否则你需要调用前缀查询来获取所有的`filed`的键值对。

> `Redis`中涉及哈希的命令还有很多, 这里并没有完全实现, 毕竟本`Lab`的主题是介绍实现`Redis`命令的设计方法, 有了上述基础命令, 其他的命令实现应该非常简单了, 都是简单重复的操作了, 有兴趣你可以自己补充其余命令

# 4 测试
在完成上面的功能后, 你可以运行下面的测试:
```bash
✗  go test ./pkg/redis/
```
此时除了之前的测试外, `TestRedisWrapperHashOperations`应该也是可以通过的
