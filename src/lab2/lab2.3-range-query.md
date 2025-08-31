 # Lab 2.3 范围查询
 # 1 概述
还记得我们对`Skiplist`实现了前缀查询和谓词查询吗, 他们本质上都是范围查询, 这一小节, 你将基于已有的`Skiplist`的前缀查询和谓词查询接口, 实现`MemTable`的谓词查询。

本小节需要你修改的代码：
-`src/memtable/memtable.cpp`
- `include/memtable/memtable.h` (Optional)

# 2 实现 iters_preffix
```go
HeapIterator MemTable::iters_preffix(const std::string &preffix,
                                     uint64_t tranc_id) {

  // TODO Lab 2.3 MemTable 的前缀迭代器

  return {};
}
```
你需要借助`Skiplist`的`begin_preffix`完成这个`MemTable::iters_preffix`, 你可以从返回值类型推断出, 我们仍然需要借助`HeapIterator`进行去重和排序。

这里需要注意的还是自定义的排序`id`(就是`SearchItem`里面的成员变量`idx_`), 你需要在构造`HeapIterator`手动赋予`idx_`正确的整型值。

另外，`tranc_id`相关的滤除操作你可以暂时忽略, 直接传入`SearchItem`的构造函数即可。

> 需要注意的是, 这个返回的迭代器从语义上是`begin`迭代器, 其使用方式是判断自身是否`is_valid`()以及`is_end()`, 不同于`C++ STL`中给定一对迭代器确定范围的风格。这也算是作者前期项目设计的不足之处，介于次代码和实验还是初版，能用能跑就行。

# 3 实现 iters_monotony_predicate
```go
std::optional<std::pair<HeapIterator, HeapIterator>>
MemTable::iters_monotony_predicate(
    uint64_t tranc_id, std::function<int(const std::string &)> predicate) {
  // TODO Lab 2.3 MemTable 的谓词查询迭代器起始范围
  return std::nullopt;
}
```

和`iters_preffix`类似, 只不过查询逻辑从特化的前缀查询变成了适用性更广泛的谓词查询, 注意事项也都差不多, 同样是借助`Skiplist`的`iters_monotony_predicate(predicate)`获取初步的结果, 再用`HeapIterator`区中。

# 4 测试
完成上面的函数后, 你应该可以通过所有的test/test_memtable.cpp的单元测试:
```bash
✗ xmake
✗ xmake run test_memtable
[==========] Running 12 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 12 tests from MemTableTest
[ RUN      ] MemTableTest.BasicOperations
[       OK ] MemTableTest.BasicOperations (0 ms)
[ RUN      ] MemTableTest.RemoveOperations
[       OK ] MemTableTest.RemoveOperations (0 ms)
[ RUN      ] MemTableTest.FrozenTableOperations
[       OK ] MemTableTest.FrozenTableOperations (0 ms)
[ RUN      ] MemTableTest.LargeScaleOperations
[       OK ] MemTableTest.LargeScaleOperations (1 ms)
[ RUN      ] MemTableTest.MemorySizeTracking
[       OK ] MemTableTest.MemorySizeTracking (0 ms)
[ RUN      ] MemTableTest.MultipleFrozenTables
[       OK ] MemTableTest.MultipleFrozenTables (0 ms)
[ RUN      ] MemTableTest.IteratorComplexOperations
[       OK ] MemTableTest.IteratorComplexOperations (0 ms)
[ RUN      ] MemTableTest.ConcurrentOperations
[       OK ] MemTableTest.ConcurrentOperations (604 ms)
[ RUN      ] MemTableTest.PreffixIter
[       OK ] MemTableTest.PreffixIter (0 ms)
[ RUN      ] MemTableTest.IteratorPreffix
[       OK ] MemTableTest.IteratorPreffix (0 ms)
[ RUN      ] MemTableTest.ItersPredicate_Base
[       OK ] MemTableTest.ItersPredicate_Base (0 ms)
[ RUN      ] MemTableTest.ItersPredicate_Large
[       OK ] MemTableTest.ItersPredicate_Large (13 ms)
[----------] 12 tests from MemTableTest (620 ms total)

[----------] Global test environment tear-down
[==========] 12 tests from 1 test suite ran. (620 ms total)
[  PASSED  ] 12 tests.
```

到此为止, `Lab2`的实验结束, 恭喜你完成本实验!


