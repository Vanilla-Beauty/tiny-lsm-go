# Lab 4.7 复杂查询
# 1 概述
其实也就是我们每个组件实现过程中最后实现的范围查询, 只是我们之前简单的`begin`和`end`也是需要实现`Level_Iterator`, 因此这里就统称其为`复杂查询`了。

本小节你需要更改的代码文件为:
- `src/lsm/engine.cpp`
- `include/lsm/engine.h` (Optional)

## 2 全局迭代器
现在你已经有了各种各样的迭代器，那么`LSMEngine`的`begin/end`自然也不在话下了:
```go
Level_Iterator LSMEngine::begin(uint64_t tranc_id) {
  // TODO: Lab 4.7
  throw std::runtime_error("Not implemented");
}

Level_Iterator LSMEngine::end() {
  // TODO: Lab 4.7
  throw std::runtime_error("Not implemented");
}
```
这里只需要简单地调用`Level_Iterator`的构造函数即可

## 3 范围查询
最后, 比全局迭代器稍微复杂的是谓词查询:
```go
std::optional<std::pair<TwoMergeIterator, TwoMergeIterator>>
LSMEngine::lsm_iters_monotony_predicate(
    uint64_t tranc_id, std::function<int(const std::string &)> predicate) {
  // TODO: Lab 4.7 谓词查询
  return std::nullopt;
}
```

这里的复杂点在于, 这是一个顶层的范围查询, 你需要完成的是所有组件迭代器的组合、排序和滤除。你需要灵活地运用我们已经实现的各种迭代器, 完成这个复杂的查询。

> 这个函数你应该能体会到我们实验代码涉及中迭代器复用的精妙之处

# 4 测试
现在你应该能通过`test_lsm`的大部分测试了:
```bash
✗ xmake
[100%]: build ok, spent 1.389s
✗ xmake run test_lsm
[==========] Running 9 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 9 tests from LSMTest
[ RUN      ] LSMTest.BasicOperations
[       OK ] LSMTest.BasicOperations (1007 ms)
[ RUN      ] LSMTest.Persistence
[       OK ] LSMTest.Persistence (2022 ms)
[ RUN      ] LSMTest.LargeScaleOperations
[       OK ] LSMTest.LargeScaleOperations (1021 ms)
[ RUN      ] LSMTest.MixedOperations
[       OK ] LSMTest.MixedOperations (1043 ms)
[ RUN      ] LSMTest.IteratorOperations
[       OK ] LSMTest.IteratorOperations (1001 ms)
[ RUN      ] LSMTest.MonotonyPredicate
[       OK ] LSMTest.MonotonyPredicate (1019 ms)
# 其余测试与实物相关
```

# 5 总结与思考
这应该是目前为止强度最大的一个阶段的`Lab`了, 不过现在你已经得到了可以运行并执行部分复杂功能的`LSM`引擎了, 所以这一阶段标志着`LSM Tree`项目的初步阶段, 算是一个小小的里程碑, 有没有一点成就感?

不过我们的`Lab`还需要继续, 首先思考着几个问题:

- 请观察你自己的`LSMTest.Persistence`的测试耗时, 是否比我的速度慢很多? 为什么? (如果你比我的速度快, 应该是硬件碾压了 🥵)
- 如果你的`LSMTest.Persistence`确实慢, 是不是缺少缓存池的优化?
- 我们的`compact`策略太粗糙, 你是否有更好的策略?

然后, 进入阶段3: [阶段3-LSM Tree 的优化](./Optimization.md)