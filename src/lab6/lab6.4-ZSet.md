# Lab 6.4 有序集合
# 1 Redis ZSet 实现思路
## 1.1 实现思路
这里也不介绍`Redis zset`的语法了, 既然看这篇文章, 相比大家对`Redis`非常熟悉了, 这里本实验选择实现如下常见的`api`:

- `zadd`
- `zrem`
- `zrange`
- `zcard`
- `zscore`
- `zincrby`
- `zrank`


Redis的有序集合(ZSet)功能通过以下方式实现：
1. 使用两个维度存储数据：成员到分数的映射和分数到成员的映射
2. 为每个成员维护两个键值对：一个存储成员对应的分数，一个存储分数对应的成员
3. 通过组合键命名策略来组织数据，实现按分数排序的功能

这种设计的核心思想是通过双向映射来实现有序集合的功能，利用KV存储的键有序特性来实现按分数排序。

## 2 具体实现方式

### 2.1 数据存储结构

ZSet的存储结构包括三个部分：
1. 元数据键：存储集合的前缀信息
2. 分数-成员映射：每个分数对应一个成员
3. 成员-分数映射：每个成员对应一个分数

```bash
# 元数据键: 存储集合前缀
key -> "__zset__:key_"

# 分数-成员映射: 分数对应的成员
"__zset__:key_SCORE_0000000001" -> "member1"
"__zset__:key_SCORE_0000000002" -> "member2"
"__zset__:key_SCORE_0000000003" -> "member3"

# 成员-分数映射: 成员对应的分数
"__zset__:key_ELEM_member1" -> "1"
"__zset__:key_ELEM_member2" -> "2"
"__zset__:key_ELEM_member3" -> "3"

# 过期键（如果设置了过期时间）
"__expire__:key" -> "timestamp"
```

### 2.2 ZADD 命令实现

ZADD命令用于向有序集合中添加一个或多个成员，或更新已存在成员的分数，实现步骤如下：
1. 检查有序集合是否过期，如果过期则清理
2. 初始化有序集合（如果不存在）
3. 对于每个要添加的成员-分数对：
   - 检查成员是否已存在
   - 如果存在，则删除旧的分数-成员映射
   - 创建新的分数-成员映射和成员-分数映射
   - 更新添加计数

### 2.3 ZREM 命令实现

ZREM命令用于从有序集合中移除一个或多个成员，实现步骤如下：
1. 检查有序集合是否过期
2. 对于每个要移除的成员：
   - 获取成员的分数
   - 删除成员-分数映射和对应的分数-成员映射
   - 更新移除计数

### 2.4 ZRANGE 命令实现

ZRANGE命令用于返回有序集合中指定范围的成员，实现步骤如下：
1. 检查有序集合是否过期
2. 通过前缀扫描获取所有分数-成员键
3. 对键进行排序（键本身已按分数排序）
4. 提取指定范围的成员
5. 返回成员列表

### 2.5 ZCARD 命令实现

ZCARD命令用于获取有序集合的成员数量，实现步骤如下：
1. 检查有序集合是否过期
2. 通过前缀扫描获取所有分数-成员键
3. 返回键的数量

### 2.6 ZSCORE 命令实现

ZSCORE命令用于返回有序集合中指定成员的分数，实现步骤如下：
1. 检查有序集合是否过期
2. 构造成员-分数键(`__zset__:key_ELEM_member`)
3. 获取并返回对应的分数值

### 2.7 ZINCRBY 命令实现

ZINCRBY命令用于为有序集合中的成员分数加上增量，实现步骤如下：
1. 检查有序集合是否过期
2. 获取成员当前分数
3. 计算新分数
4. 删除旧的分数-成员映射
5. 创建新的分数-成员映射和成员-分数映射

### 2.8 ZRANK 命令实现

ZRANK命令用于返回有序集合中指定成员的排名，实现步骤如下：
1. 检查有序集合是否过期
2. 获取成员的分数
3. 通过前缀扫描获取所有分数-成员键并排序
4. 查找成员在排序列表中的位置

## 3 数据结构设计图示
```bash
有序集合 ZSet 结构示意图:

+----------------+       +---------------------+
|   元数据键     |       |     过期键          |
| key -> prefix  |       | __expire__:key -> t |
+----------------+       +---------------------+
         |
         v
+----------------+       +----------------+
| 成员-分数映射  |<----->| 分数-成员映射  |
| ELEM_member1   |       | SCORE_00000001 |
|     -> "1"     |       |     -> member1 |
+----------------+       +----------------+
| ELEM_member2   |<----->| SCORE_00000002 |
|     -> "2"     |       |     -> member2 |
+----------------+       +----------------+
| ELEM_member3   |<----->| SCORE_00000003 |
|     -> "3"     |       |     -> member3 |
+----------------+       +----------------+
```

## 4 具体示例

假设我们要创建一个名为"rankings"的有序集合，用于存储游戏排行榜：

```bash
# 添加玩家和分数
ZADD rankings 100 "player1"
ZADD rankings 200 "player2" 
ZADD rankings 150 "player3"

# 此时存储结构如下:
rankings -> "__zset__:rankings_"
"__zset__:rankings_SCORE_0000000100" -> "player1"
"__zset__:rankings_SCORE_0000000150" -> "player3"
"__zset__:rankings_SCORE_0000000200" -> "player2"
"__zset__:rankings_ELEM_player1" -> "100"
"__zset__:rankings_ELEM_player2" -> "200"
"__zset__:rankings_ELEM_player3" -> "150"
```

由于键是按字典序排列的，所以当我们扫描"__zset__:rankings_SCORE_"前缀时，会自然地按分数从低到高排序：
1. player1 (100分)
2. player3 (150分)
3. player2 (200分)

这种设计充分利用了底层KV存储的有序特性，通过巧妙的键命名策略实现了有序集合的功能。每个成员通过两个键值对来维护，保证了可以快速地通过成员查找分数，也可以通过分数范围查找成员。

# 5 代码实现
通过上面的理论讲解和案例说明, 你应该对此非常熟悉了...

你需要修改的代码文件包括:
- `src/redis_wrapper/redis_wrapper.cpp`
- `include/redis_wrapper/redis_wrapper.h` (Optional)

> 下面的接口中, 你仍然需要进行`TTL`超时时间的判断, 同时你可能需要更新之前的`redis_ttl`和`redis_expire`以兼容`ZSet`的`TTL`机制。

首先, 推荐你也先实现过期数据的清理函数:
```go
// pkg/redis/zset_commands.go
// expireCleanZSet checks and cleans expired sorted set data
func (r *RedisWrapper) expireCleanZSet(key string) bool {
	// TODO: Lab 6.4

	return false
}
```
另外你也许会利用到`func (r *RedisWrapper) extractScoreFromKey(scoreKey string) string`这个函数从`score`的`key`中提取去除前缀的`score`的值

同时, `pkg/redis/redis_wrapper.go`也提供了一些帮助函数:
- `func (r *RedisWrapper) getSortedSetPrefix(key string) string`: 生成有序集合的通用前缀
- `func (r *RedisWrapper) getSortedSetScorePrefix(key string) string`: 生成有序集合中分数相关键的前缀
- `func (r *RedisWrapper) getSortedSetElemKey(key, elem string) string` 生成有序集合中成员存储的键
- `func (r *RedisWrapper) getSortedSetScoreKey(key, score string) string`: 生成有序集合中分数存储的键

## 3.1 zadd
```go
func (r *RedisWrapper) ZAdd(args []string) string {
	// TODO: Lab 6.4

	addedCount := 0
	return fmt.Sprintf(":%d\r\n", addedCount)
}
```
注意, 这里支持一次性添加多个`filed`, 返回值表示多少个`filed`添加成功

## 3.2 zrem
```go
// ZRem implements Redis ZREM command
func (r *RedisWrapper) ZRem(args []string) string {
	// TODO: Lab 6.4

	removedCount := 0
	return fmt.Sprintf(":%d\r\n", removedCount)
}
```
注意, 这里支持一次性删除多个`filed`, 返回值表示多少个`filed`删除成功

## 3.3 zrange
```go
// ZRange implements Redis ZRANGE command
func (r *RedisWrapper) ZRange(args []string) string {
	// TODO: Lab 6.4

	result := ""
	return result
}
```
查询指定范围的元素, 返回值表示查询到的元素的数组(`RESP`格式), 你需要使用`LSM`的谓词查询接口

## 3.4 zcard
```go
// ZCard implements Redis ZCARD command
func (r *RedisWrapper) ZCard(args []string) string {
	// TODO: Lab 6.4

	return ":0\r\n"
}
```
查询有序集合的元素个数

## 3.5 zscore
```go
// ZScore implements Redis ZSCORE command
func (r *RedisWrapper) ZScore(args []string) string {
	// TODO: Lab 6.4

	return "$len(scoreValue)\r\n%scoreValue\r\n"
}
```
查询有序集合中指定`filed`的分数, 你只需要按照指定格式拼接`key`进行查询即可

## 3.6 zincrby
```go
// ZIncrBy implements Redis ZINCRBY command
func (r *RedisWrapper) ZIncrBy(args []string) string {
	// TODO: Lab 6.4

	return ":%newScoreStr\r\n"
}
```
对有序集合中指定元素的分数进行增加, 返回值表示增加后的分数

## 3.7 zrank
```go
// ZRank implements Redis ZRANK command
func (r *RedisWrapper) ZRank(args []string) string {
	// TODO: Lab 6.4

	return "$rank_num\r\n"
}
```
获取有序集合中指定元素的排名, 返回值表示排名

# 4 测试
在完成上面的功能后, 你可以运行下面的测试:
```bash
✗  go test ./pkg/redis/
```
此时除了之前的测试外, `TestRedisWrapperZSetOperations`应该也是可以通过的