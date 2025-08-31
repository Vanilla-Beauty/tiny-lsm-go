# Lab 1.3 范围查询
# 1 范围查询的特性
根据之前的介绍我们了解到, 我们实现的跳表是一个有序数据结构, 而我们构建的数据库是`K00V00`数据库, 因此除了最基础的`CRUD`操作外, 我们还需要实现一个范围查询的功能。这些范围查询包括:
1. 前缀查询: 通过前缀查询, 我们可以查询以某个前缀开头的所有键值对。比如, 我们数据库中如果`k00ey`用`userxxx`标识用户数据, 可以查询以`"userxx"`开头的所有用户数据的键值对。
2. 范围查询: 通过范围查询, 我们可以查询某个范围内的键值对。比如, 我们数据库中如果`k00ey`是学生的学号, 那么我们可以查询某个学号范围内的学生数据`[100, 200)`。

以上的查询都存在一个特性: 他们是单调的查询, 也就是说在全局数据库中只会出现一个这样的区间。

例如我们有下面的键值对:
```text
(k001, v001), (k002, v002), (k003, v003), (k004, v004), (k005, v005), (k006, v006), (k007, v007), (k008, v008), (k009, v009), (k010, v010)
```
- 我们查询`key >= k005 && key < k008`的范围, 我们可以得到:
```text
(k005, v005), (k006, v006), (k007, v007)`。
```

- 我们查询`key前缀为k00`的键值对, 我们可以得到:
```text
`(k001, v001), (k002, v002), (k003, v003), (k004, v004), (k005, v005), (k006, v006), (k007, v007), (k008, v008), (k009, v009)`。
```

可以看到, 这些查询都是单调的, 也就是说, 全局数据库中只会出现一个这样的区间。而考虑到我们的数据库是有序的, 因此可以使用二分查询的方式, 确定查询区间的开始位置和结束位置, 以迭代器的形式返回查询结果即可。

# 2 实现
## 2.1 前缀查询
你需要实现这两个函数：
```go
// 找到前缀的起始位置
// 返回第一个前缀匹配或者大于前缀的迭代器
SkipListIterator SkipList::begin_preffix(const std::string &preffix) {
  // TODO: Lab1.3 任务：实现前缀查询的起始位置
  return SkipListIterator{};
}

// 找到前缀的终结位置
SkipListIterator SkipList::end_preffix(const std::string &prefix) {
  // TODO: Lab1.3 任务：实现前缀查询的终结位置
  return SkipListIterator{};
}
```
注意的是, 这里迭代器返回的定义类似`STL`中迭代器`end`, 其并不属于指定的区间类, 也就是说区间的数学表达是`[begin, end)`

## 2.2 谓词查询
这里除了前缀查询外, 还包括各种其他的查询, 只要他们是单调的即可。因此我们给出了一种更通用的接口, 谓词查询, 只需要上层调用者提供一个谓词(`lambda`函数或者仿函数均可), 我们就可以实现各种单调区间的范围查询。

因此, 我们可以设计这样一个查询接口, 其接收一个谓词, 这个谓词的具体函数体说明此次查询是普通的范围查询、前缀匹配或者是其他的单条区间查询, 但要求结果一定在全局只位于一个连续区间中就可以。同时该谓词不能返回`bool`值, 而是类似字符串比较那样返回一个`int`值, 0 表示不匹配, 1 表示大于, -1 表示小于。这样我们才可以根据返回值确定下一步二分查找的方向。

我们需要逐层次实现这个支持谓词的查询接口，其返回一组迭代器表示`start`和`end`, 这里我们还是要利用`SkipList`的有序性多层不同步长的链表来实现快速的匹配查询。

你需要实现下面的函数:
```go
// ? 这里单调谓词的含义是, 整个数据库只会有一段连续区间满足此谓词
// ? 例如之前特化的前缀查询，以及后续可能的范围查询，都可以转化为谓词查询
// ? 返回第一个满足谓词的位置和最后一个满足谓词的迭代器
// ? 如果不存在, 范围nullptr
// ? 谓词作用于key, 且保证满足谓词的结果只在一段连续的区间内, 例如前缀匹配的谓词
// ? predicate返回值:
// ?   0: 满足谓词
// ?   >0: 不满足谓词, 需要向右移动
// ?   <0: 不满足谓词, 需要向左移动
// ! Skiplist 中的谓词查询不会进行事务id的判断, 需要上层自己进行判断
std::optional<std::pair<SkipListIterator, SkipListIterator>>
SkipList::iters_monotony_predicate(
    std::function<int(const std::string &)> predicate) {
  // TODO: Lab1.3 任务：实现谓词查询的起始位置
  return std::nullopt;
}
```

> Hint: 这里的思路其实也很简单, 推荐的思路如下:
>   1. 先逐次使用高`Level`的链表, 逐次降低, 找到满足谓词的区间的某一个节点`node1`
>   2. 根据找到的节点`node1`, 逐次向左移动, 直到找到第一个满足谓词的节点`node0`
>   3. 根据找到的节点`node1`, 逐次向右移动, 直到找到最后一个满足谓词的节点`node2`
>   4. 返回找到的节点`node0`和`node2`

> 看到这里你应该也明白了, 为什么实验的`Skiplist`的头文件定义采用了双向链表, 目的就是方便这里谓词查询链表节点方向移动的便捷性


# 3 测试
完成上面的函数后, 你应该可以通过除了`SkipListTest.TransactionId`外所有的`test/test_skiplist.cpp`的单元测试:
```go
✗ xmake
[100%]: build ok, spent 5.785s
✗ xmake run test_skiplist
[==========] Running 12 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 12 tests from SkipListTest
[ RUN      ] SkipListTest.BasicOperations
[       OK ] SkipListTest.BasicOperations (0 ms)
[ RUN      ] SkipListTest.LargeScaleInsertAndGet
[       OK ] SkipListTest.LargeScaleInsertAndGet (7 ms)
[ RUN      ] SkipListTest.LargeScaleRemove
[       OK ] SkipListTest.LargeScaleRemove (6 ms)
[ RUN      ] SkipListTest.DuplicateInsert
[       OK ] SkipListTest.DuplicateInsert (0 ms)
[ RUN      ] SkipListTest.EmptySkipList
[       OK ] SkipListTest.EmptySkipList (0 ms)
[ RUN      ] SkipListTest.RandomInsertAndRemove
[       OK ] SkipListTest.RandomInsertAndRemove (6 ms)
[ RUN      ] SkipListTest.MemorySizeTracking
[       OK ] SkipListTest.MemorySizeTracking (0 ms)
[ RUN      ] SkipListTest.Iterator
[       OK ] SkipListTest.Iterator (0 ms)
[ RUN      ] SkipListTest.IteratorPreffix
[       OK ] SkipListTest.IteratorPreffix (0 ms)
[ RUN      ] SkipListTest.ItersPredicate_Base
[       OK ] SkipListTest.ItersPredicate_Base (0 ms)
[ RUN      ] SkipListTest.ItersPredicate_Large
[       OK ] SkipListTest.ItersPredicate_Large (5 ms)
[ RUN      ] SkipListTest.TransactionId^
```

> `SkipListTest.TransactionId`单元测试是`Lab 5.1`的内容, 你可以先忽略它。

此外, 单元测试目前并没有规定你的实现的效率, 但你的实现在`release`模式下, 不应该超过`100ms`, (`30ms`以内最佳)

到此为止, `Lab1`的实验结束, 恭喜你完成本实验!

