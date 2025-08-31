# Lab 3.3 范围查询

# 1 范围查询函数
鉴于你之前已经在`Skiplist`组件和`MemTable`组件中实现了`range_query`功能, 这里我们需要再`Blcok`组件中再次实现`range_query`功能。(同样, 查询是单调的), 只不过这里操作的基础数据从内存中的跳表变成了类似数组结构的`Block`。

你需要修改的文件:
- `src/block/block.cpp`
- `include/block/block.h` (Optional)

## 1.1 前缀查询
具体修改的函数为:
```go
std::optional<
    std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
Block::iters_preffix(uint64_t tranc_id, const std::string &preffix) {
  // TODO Lab 3.2 获取前缀匹配的区间迭代器
  return std::nullopt;
}
```
这里返回一对迭代器,标识前缀匹配的区间。(同样是和`STL`风格一致的左闭右开区间), 如果查询不到, 返回`std::nullopt`。

> `std::optional`是一个智能指针, 其用法非常类似`Rust`的`Option`

## 1.2 谓词查询
具体修改的函数为:
```go
// 返回第一个满足谓词的位置和最后一个满足谓词的位置
// 如果不存在, 范围nullptr
// 谓词作用于key, 且保证满足谓词的结果只在一段连续的区间内, 例如前缀匹配的谓词
// 返回的区间是闭区间, 开区间需要手动对返回值自增
// predicate返回值:
//   0: 满足谓词
//   >0: 不满足谓词, 需要向右移动
//   <0: 不满足谓词, 需要向左移动
std::optional<
    std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
Block::get_monotony_predicate_iters(
    uint64_t tranc_id, std::function<int(const std::string &)> predicate) {
  // TODO: Lab 3.2 使用二分查找获取满足谓词的区间迭代器
  return std::nullopt;
}
```
这里返回一对迭代器,标识谓词查询的区间。(同样是和`STL`风格一致的左闭右开区间), 如果查询不到, 返回`std::nullopt`。

# 2 测试
如下运行测试， 预期结果为：
```bash
✗ xmake
✗ xmake run test_block
[==========] Running 10 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 10 tests from BlockTest
[ RUN      ] BlockTest.DecodeTest
[       OK ] BlockTest.DecodeTest (0 ms)
[ RUN      ] BlockTest.EncodeTest
[       OK ] BlockTest.EncodeTest (0 ms)
[ RUN      ] BlockTest.BinarySearchTest
[       OK ] BlockTest.BinarySearchTest (0 ms)
[ RUN      ] BlockTest.EdgeCasesTest
[       OK ] BlockTest.EdgeCasesTest (0 ms)
[ RUN      ] BlockTest.LargeDataTest
[       OK ] BlockTest.LargeDataTest (0 ms)
[ RUN      ] BlockTest.ErrorHandlingTest
[       OK ] BlockTest.ErrorHandlingTest (1 ms)
[ RUN      ] BlockTest.IteratorTest
[       OK ] BlockTest.IteratorTest (0 ms)
[ RUN      ] BlockTest.PredicateTest
[       OK ] BlockTest.PredicateTest (2 ms) # 到这里成功就表示你完成了本`Lab`
[ RUN      ] BlockTest.TrancIteratorTest
[       OK ] BlockTest.TrancIteratorTest (0 ms)
[ RUN      ] BlockTest.TrancPredicateTest
[       OK ] BlockTest.TrancPredicateTest (0 ms)
[----------] 10 tests from BlockTest (4 ms total)

[----------] Global test environment tear-down
[==========] 10 tests from 1 test suite ran. (4 ms total)
[  PASSED  ] 10 tests.
```
其中最后两个测试`BlockTest.TrancIteratorTest`和`BlockTest.TrancPredicateTest`需要后续实现事务功能后才能正常通过。只要你通过了`BlockTest.PredicateTest`前的测试, 即视为完成了本`Lab`。