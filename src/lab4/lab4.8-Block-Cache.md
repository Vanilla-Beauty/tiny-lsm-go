# Lab 4.8 复杂查询

# 1 缓存池原理与设计
# 1.1 缓存池的作用

在LSM Tree的实现中，数据读取是以`Block`为单位进行的。为了提高热点`Block`的读取效率，我们引入了缓存池（`Buffer Pool`）。缓存池的主要作用主要是减少磁盘I/O, 通过将频繁访问的`Block`缓存到内存中，可以显著减少对磁盘的读取次数，从而提高系统的整体性能。因此这里我们的目的就是尽量让热点`Block`数据常驻内存。

## 1.2 LRU-K算法原理
这里我采用`LRU-K`算法来管理缓存池。其实主要原因是以前做过`CMN15445`, 里面实现过`LRU-K`算法, 稍作改进可以直接拿来用, 这里就偷个懒了。照例还是先简单介绍下`LRU-K`。

`LRU-K`（Least Recently Used with K accesses）是一种基于访问频率和时间的缓存淘汰策略。相比于传统的`LRU`（最近最少使用），`LRU-K`考虑了更复杂的访问模式，能够更好地适应实际应用场景中的数据访问特性。`LRU-K`仍然是使用键值对来索引缓存内容。

**设计思路**
- Key：表示在决定淘汰某个缓存项时，会考虑该缓存项在过去K次访问的时间间隔。当K=1时，LRU-K退化为普通的LRU。这里我的的`Blcok`可以由`(sst_id, block_id)`唯一索引, 因此缓存池的的`Block`索引键可以设计为`std::pair<sst_id, block_id>`
- 访问链表：每个缓存项都有一个访问历史记录，记录其最近K次的访问时间。当最近访问次数大于等于K次, 则放在链表首部, 不足K次的放在后半部分, 按照访问时间排序。

**淘汰规则**

- 当缓存池满时，选择那些在过去K次访问中时间间隔最长的缓存项进行淘汰。
- 如果多个缓存项的时间间隔相同，则选择最早插入缓存池的那个项。

# 2 代码实现
## 2.1 代码思路梳理
这里采用一个简化的`LRU-K`实现, 我们不需要记录访问的时间戳, 而是用链表的顺序来表达最新的访问记录, 最新的访问置于链表头部。另一方面，为保证快速查询，我们还需要一个哈希表，这个哈希表的索引键为`std::pair<sst_id, block_id>`， 索引值为链表的迭代器， 因此结合二者可以简单实现查询和新增均为`O(1)`的缓存池。那怎么表达`LRU-K`呢? 很简单, 采用2个链表, 一个存储访问次数少于`k`次的缓存项, 一个存储访问次数大于等于`k`次的缓存项。

首先看一下头文件的关键定义:
```go
struct CacheItem {
  int sst_id;
  int block_id;
  std::shared_ptr<Block> cache_block;
  uint64_t access_count; // 访问计数
};
// 定义缓存池
class BlockCache {
public:
  ...

private:
  size_t capacity_;          // 缓存容量
  size_t k_;                 // LRU-K 中的 K 值

  // 双向链表存储缓存项
  std::list<CacheItem> cache_list_greater_k;
  std::list<CacheItem> cache_list_less_k;

  // 哈希表索引缓存项
  std::unordered_map<std::pair<int, int>, std::list<CacheItem>::iterator,
                     pair_hash, pair_equal>
      cache_map_;

  ...
};
```
<details>
<summary>点击这里展开/折叠提示 (建议你先尝试自己完成)</summary>

> 本思路实现的`LRU-K`并不标准, 你可以按照自己的方案实行, 可以大刀阔斧地修改代码, 保证接口统一即可

我们结合`cache_list_greater_k`, `cache_list_less_k`和`cache_map_`来整理一下流程:

**查询**
1. 先通过`cache_map_`查询缓存项的迭代器, 通过迭代器的访问次数判断其属于哪个链表。
   1. `访问次数 < k` -> 属于 `cache_list_less_k`
      1. 如果`访问次数 == k-1`，更新访问次数后就就等于k了，将其重新置于`cache_list_greater_k`的头部。
      2. 否则更新后仍然属于`cache_list_greater_k`，但需要将其移动到`cache_list_greater_k`的头部。
   2. `访问次数 = k` -> 属于 `cache_list_greater_k`, 将其置于`cache_list_greater_k`的头部。但`访问次数`就固定在`k`, 不继续增长了

**插入**
1. 先通过`cache_map_`查询缓存项是否存在, 存在则按照之前的步骤更新缓存项即可
2. 不存在的话, 判断缓存池是否已满
   1. 没有满
      1. 直接插入到`cache_list_less_k`头部，并更新cache_map_索引。
   2. 满了
      1. 如果`cache_list_less_k`不为空，从`cache_list_less_k`末尾淘汰一个缓存项，并插入新的缓存项到`cache_list_less_k`头部。
      2. 如果`cache_list_less_k`为空，从`cache_list_greater_k`末尾淘汰一个缓存项，并插入新的缓存项到`cache_list_less_k`。

</details>

> 根据你的实现, 你可以更改`CacheItem`的定义和排序规则, 例如记录访问的具体时间戳等

# 2.2 具体实现
你需要修改的代码文件包括:
- `src/block/block_cache.cpp`
- `include/block/block_cache.h` (Optional)

要实现的函数也很简单, 首先是插入一个`Block`到缓存池:
```go
void BlockCache::put(int sst_id, int block_id, std::shared_ptr<Block> block) {
  // TODO: Lab 4.8 插入一个 Block
}
```

然后是查询一个`Block`:
```go
std::shared_ptr<Block> BlockCache::get(int sst_id, int block_id) {
  // TODO: Lab 4.8 查询一个 Block
  return nullptr;
}
```

最后是一些辅助函数, 比如你在插入和查询时, 需要更新相应`Block`的统计信息:
```go
void BlockCache::update_access_count(std::list<CacheItem>::iterator it) {
  // TODO: Lab 4.8 更新统计信息
}
```

具体更新什么统计信息呢? 除了要保证缓存池的基础运行逻辑正确外, 你看到这个获取命中率的函数可能有所启发:
```go
double BlockCache::hit_rate() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return total_requests_ == 0
             ? 0.0
             : static_cast<double>(hit_requests_) / total_requests_;
}
```

# 3 缓存池的构造
现在我们已经实现了`BlockCache`这个类, 你应该还记得, 之前我们许多函数的参数中都包含了`std::shared_ptr<BlockCache>`类型的参数, 且其都发起于上层组件`LSMEngine`, 同时`LSMEngine`也包含了`std::shared_ptr<BlockCache>`的成员变量, 因此你需要在`LSMEngine`的构造函数中初始化这个成员变量, 并在调用的函数中进行传递。

> 一些配置参数仍然可以查看`config.toml`并使用`TomlConfig::getInstance().getXXX`获取

# 4 测试
对于缓存池本身, 你应该可以通过下面的测试:
```bash
✗ xmake
✗ xmake run test_block_cache
[==========] Running 4 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 4 tests from BlockCacheTest
[ RUN      ] BlockCacheTest.PutAndGet
[       OK ] BlockCacheTest.PutAndGet (0 ms)
[ RUN      ] BlockCacheTest.CacheEviction1
[       OK ] BlockCacheTest.CacheEviction1 (0 ms)
[ RUN      ] BlockCacheTest.CacheEviction2
[       OK ] BlockCacheTest.CacheEviction2 (0 ms)
[ RUN      ] BlockCacheTest.HitRate
[       OK ] BlockCacheTest.HitRate (0 ms)
[----------] 4 tests from BlockCacheTest (0 ms total)

[----------] Global test environment tear-down
[==========] 4 tests from 1 test suite ran. (0 ms total)
[  PASSED  ] 4 tests.
```

在`SSt`和`Block`的逻辑层面, 本节实验并未新添加任何接口, 只是做出了IO层面的优化, 因此, 你只需要像[Lab 4.7 复杂查询](./lab4.7-Complicated-Query.md)那样再次运行单元测试, 观察前后数据读取的效率变化即可。

需要说明的是, 你需要尤其关注`Persistence`这个测例(位于`test/test_lsm.cpp`), 因为其会插入大量的数据并刷盘, 此时的大部分查询请求都是在`SST`文件层面进行的, 能够较为明显地观察出加入缓存池优化后的速度变化。
