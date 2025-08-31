# Lab 3.7 范围查询

# 1 函数功能描述
同样地，我们设计了`sst_iters_monotony_predicate`函数，用于范围一段连续的区间, 这个区间是单调的, 只会在整个`SST`中出现一次, 例如包含指定前缀的一段范围。

不过这里我们将次函数定义为静态函数, 并放置于`src/sst/sst_iterator.cpp`中。因为我们之前进行了模块拆分， `SST`和`SstIterator`分别定义在了`src/sst/sst.cpp`和`src/sst/sst_iterator.cpp`中，而`sst_iters_monotony_predicate`函数需要同时访问`SST`和`SstIterator`，因此我们需要将`sst_iters_monotony_predicate`定义为静态函数, 并设定为以上两个类的友元函数.

# 2 代码实现
你需要更改的文件:
- `src/sst/sst_iterator.cpp`

你需要实现`sst_iters_monotony_predicate`函数:
```go
// predicate返回值:
//   0: 谓词
//   >0: 不满足谓词, 需要向右移动
//   <0: 不满足谓词, 需要向左移动
std::optional<std::pair<SstIterator, SstIterator>> sst_iters_monotony_predicate(
    std::shared_ptr<SST> sst, uint64_t tranc_id,
    std::function<int(const std::string &)> predicate) {
  // TODO: Lab 3.7 实现谓词查询功能
  return {};
}
```

> Hint
> - 这里的实现思路肯定是调用子组件`Block`的`get_monotony_predicate_iters`接口实现范围查询, 但你需要考虑不同`Block`查询结果的拼接, 即查询的目标可能跨`Block`分布
> - `SST`中所有的`Block`都是有序的, 因此你在定位`Block`时, 也推荐使用类似二分查询的思路加快定位速度
> - `src/sst/sst_iterator.cpp`中的`sst_iters_monotony_predicate`已经被设定为了`SST`和`SstIterator`的友元函数, 因此你可以随意访问`SST`和`SstIterator`的成员变量和函数, 这样应该可以简化你的实现

# 3 测试 && 阶段2 结束
现在, 你应该可以完成`test_sst`的所有测例:
```bash
✗ xmake
✗ xmake run test_sst
[==========] Running 8 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 8 tests from SSTTest
[ RUN      ] SSTTest.BasicWriteAndRead
[       OK ] SSTTest.BasicWriteAndRead (2 ms)
[ RUN      ] SSTTest.BlockSplitting
[       OK ] SSTTest.BlockSplitting (0 ms)
[ RUN      ] SSTTest.KeySearch
[       OK ] SSTTest.KeySearch (0 ms)
[ RUN      ] SSTTest.Metadata
[       OK ] SSTTest.Metadata (0 ms)
[ RUN      ] SSTTest.EmptySST
[       OK ] SSTTest.EmptySST (0 ms)
[ RUN      ] SSTTest.ReopenSST
[       OK ] SSTTest.ReopenSST (0 ms)
[ RUN      ] SSTTest.LargeSST
[       OK ] SSTTest.LargeSST (0 ms)
[ RUN      ] SSTTest.LargeSSTPredicate
[       OK ] SSTTest.LargeSSTPredicate (0 ms)
[----------] 8 tests from SSTTest (6 ms total)

[----------] Global test environment tear-down
[==========] 8 tests from 1 test suite ran. (6 ms total)
[  PASSED  ] 8 tests.
```

# 4 思考 && 下一步?
现在你已经实现了`SST`的基本特性, 而剩余的`SST`特性还包括:
- 不同`Level`的`SST`的压缩合并
- 缓存池和布隆过滤器的优化
- 以`Level`层级为单位的迭代器(就是将一整个`Level`的多个`SST`组织成一个迭代器)

以上这些内容, 你将在[Lab 4 LSM Engine](../lab4/lab4-LSM-Engine.md)中实现。因为以上的组件需要上层组件的控制，例如我们的缓存池是全局共享而非单个`SST`独有的, 因此需要上层组件进行初始化和分配。
