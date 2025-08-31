# Lab 3.2 迭代器
# 1 BlockIterator的设计
现在实现我们的`Block`的迭代器。通过`Block`的编码格式可知, 只需要知道当前的索引, 就可以在`Block`中查询该索引的`kv`对, 因此迭代器只需要记录当前索引和原始`Block`的引用就可以了。

这也就是之前提到的`std::enable_shared_from_this`的作用，让我们的`BlockIterator`可以使用`Block`的智能指针, 为什么这样设计呢? 因为`BlockIterator`的生命周期是依赖于于`Block`的, 如果`Block`的生命周期结束, `BlockIterator`依然存在, 那么就会产生悬空指针, 因此我们需要使用智能指针来管理`Block`的生命周期。

这么说可能有点迷糊, 我们直接看头文件定义:
```go
class BlockIterator {
  // ...
private:
  std::shared_ptr<Block> block;                   // 指向所属的 Block
  size_t current_index;                           // 当前位置的索引
  uint64_t tranc_id_;                             // 当前事务 id
  mutable std::optional<value_type> cached_value; // 缓存当前值
};
```

> `cached_value`其实算是个小优化, 尽管有索引就在`block`中进行二分查找, 但这里还是用一个值进行缓存, 因为迭代器可能被反复读取值, 而每次读取值需要按照数据编码的格式进行解码, 在多次读取迭代器值的情况下, 缓存起来从理论上速度回更快

这里的`BlockIterator`不同于我们之前的`HeapIterator`, 它并不持有我们实际的键值对数据, 而进行对`Block`中的键值对位置进行定位, 因此就需要保证其指针`block`是有效的, 在现代`C++`中, 我们应该避免使用裸指针, 因此这里使用了`std::shared_ptr`来保证其指针`block`有效的。但` std::shared_ptr<Block> block`肯定是某个`Blcok`的`this`指针, 而`this`是裸指针, 因此我们需要使`Block`继承`std::enable_shared_from_this`, 这样就可以通过`shared_from_this()`获取代表`this`指针的`shared_ptr<Block>`了。

> 需要注意的是, 使用`shared_from_this()`时, 需要保证类的实例被`shared_ptr`管理, 否则会抛出异常, 具体可以查询相关资料, 这里不过多介绍。

其余成员变量应该很好理解, `current_index`记录当前索引, `tranc_id_`记录当前事务 id, `cached_value`缓存当前值。

# 2 实现 `BlockIterator`
本部分你需要修改的代码文件为：
- `src/block/block_iterator.cpp`
- `include/block/block_iterator.h` (Optional)

## 2.1 构造函数实现
实现构造函数，传入一个 `block` 和 `key`，以及事务 `tranc_id`, 这里在构造迭代器时就将迭代器移动到指定`key`的位置(还需要满足`tranc_id`的可见性, 不过现在你可以先忽略这个可见性的判断逻辑)。

你需要借助之前实现的`Block`的成员函数来实现这的移动逻辑:

```go
BlockIterator::BlockIterator(std::shared_ptr<Block> b, const std::string &key,
                             uint64_t tranc_id)
    : block(b), tranc_id_(tranc_id), cached_value(std::nullopt) {
  // TODO: Lab3.2 创建迭代器时直接移动到指定的key位置
  // ? 你需要借助之前实现的 Block 类的成员函数
}
```

## 2.2 运算符重载
迭代器的运算符重载是你需要实现的基础成员函数:
```go
BlockIterator::pointer BlockIterator::operator->() const {
  // TODO: Lab3.2 -> 重载
  return nullptr;
}

BlockIterator &BlockIterator::operator++() {
  // TODO: Lab3.2 ++ 重载
  // ? 在后续的Lab实现事务后，你可能需要对这个函数进行返修
  return *this;
}

bool BlockIterator::operator==(const BlockIterator &other) const {
  // TODO: Lab3.2 == 重载
  return true;
}

bool BlockIterator::operator!=(const BlockIterator &other) const {
  // TODO: Lab3.2 != 重载
  return true;
}

BlockIterator::value_type BlockIterator::operator*() const {
  // TODO: Lab3.2 * 重载
  return {};
}
```

> 这些运算符重载函数中, 你也不需要考虑`tranc_id`的相关逻辑, 只是你需要记得, 后续实现了事务功能后, 本`Lab`的部分逻辑需要进行调整

## 2.3 辅助函数
这里有一些作者提供的可能用用的辅助函数, 你可以按选择实现他们, 也可以忽略他们, 自己按照自己的理解创建自定义的成员函数:
```go
void BlockIterator::update_current() const {
  // TODO: Lab3.2 更新当前指针
  // ? 该函数是可选的实现, 你可以采用自己的其他方案实现->, 而不是使用
  // ? cached_value 来缓存当前指针
}

void BlockIterator::skip_by_tranc_id() {
  // TODO: Lab3.2 * 跳过事务ID
  // ? 只是进行标记以供你在后续Lab实现事务功能后修改
  // ? 现在你不需要考虑这个函数
}
```

这里的`skip_by_tranc_id`只是标记后续`Lab`实现事务带来的的一些逻辑上的变化, 你现在不需要实现。
而`update_current`则是一个可选的实现, 其用来更新缓存的键值对变量, 你可以采用自己的其他方案实现`->`, 而不是使用成员变量`cached_value`和这个函数。


# 4 获取迭代器的接口函数实现
现在我们已经实现了`BlockIterator`的, 我们需要实现`Block`的`begin`和`end`函数将`BlockIterator`进行返回给外部组件使用:
```go
BlockIterator Block::begin(uint64_t tranc_id) {
  // TODO Lab 3.2 获取begin迭代器
  return BlockIterator(nullptr, 0, 0);
}

BlockIterator Block::end() {
  // TODO Lab 3.2 获取end迭代器
  return BlockIterator(nullptr, 0, 0);
}
```

这里的`tranc_id`同样可以暂时忽略, 但对应类实例的值还是要在构造函数中初始化的。

# 5 测试
测试代码在`test/test_block.cpp`中, 你在完成上述组件实现后的测试结果预期为:
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
test/test_block.cpp:277: Failure
Value of: result.has_value()
  Actual: false
Expected: true

unknown file: Failure
C++ exception with description "bad optional access" thrown in the test body.
```

`PredicateTest`需要你在完成下一小节的任务后, 才能通过。

现在你可以开启下一节的[范围查询](./lab3.3-iter-query.md)
