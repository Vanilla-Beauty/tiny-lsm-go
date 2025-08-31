# Lab 4.6 Level_Iterator

# 1 概述
为什么要将`Level_Iterator`放在`Compact`之后呢? 当然是因为`Compact`之后, 我们才有了`Level`的概念, 才能对某个`Level`的所有键值对进行迭代.

`Level_Iterator`的实现其实非常简单了, 与`TwoMergeIterator`非常类似, 只不过整合的迭代器数量是不定的, 用一个`vector`存储, 我们先简单看看定义:
```go
class Level_Iterator : public BaseIterator {
  // ...

private:
  std::shared_ptr<LSMEngine> engine_;
  std::vector<std::shared_ptr<BaseIterator>> iter_vec;
  size_t cur_idx_;
  uint64_t max_tranc_id_;
  mutable std::optional<value_type> cached_value; // 缓存当前值
  std::shared_lock<std::shared_mutex> rlock_;
};
```

这里同样是有上层`LSMEngine`的智能指针`engine_`, `iter_vec`按照索引顺序存储不同优先级的迭代器, 这里的情形就是不同的`Level`的层间迭代器(`ConcactIterator`)

> 当然, 也不一定是`ConcactIterator`, 只要多个迭代器存在优先级的概念, 都可以用`ConcactIterator`进行整合。例如我们之前由于`Level 0`的不同`SST`存在重叠且为排序, 这种情况也可以用`ConcactIterator`进行整合, 因为`id`更大的`SST`优先级更高, 需要先遍历。同时也看出我们迭代器设计存在强大的复用性，因为这里的`BaseIterator`是抽象类，只要实现了`BaseIterator`的接口，都可以作为`iter_vec`的元素, 从而实现整合逻辑的复用。

# 2 代码实现
相比上一章, 这一章轻松不少, 只需要实现几个简单的重载和构造函数初始化流程即可

本小节你需要修改的代码文件:
- `src/lsm/level_iterator.cpp`
- `include/lsm/level_iterator.h` (Optional)

## 2.1 迭代器初始化
```go
Level_Iterator::Level_Iterator(std::shared_ptr<LSMEngine> engine,
                               uint64_t max_tranc_id)
    : engine_(engine), max_tranc_id_(max_tranc_id), rlock_(engine_->ssts_mtx) {
  // TODO: Lab 4.6 Level_Iterator 初始化
}
```

这里的初始化流程就是提取每一个`Level`的迭代器然后放入`iter_vec`中, 不过同样的, `Level 0`的`SST`由于存在重叠且未排序, 需要进行额外处理。

> 思考: 初始化`iter_vec`后, 是否需要进行一些额外的判断呢?

## 2.2 运算符重载
接下来就是我们的传统艺能————运算符函数重载了:
```go
BaseIterator &Level_Iterator::operator++() {
  // TODO: Lab 4.6 ++ 重载
  return *this;
}

bool Level_Iterator::operator==(const BaseIterator &other) const {
  // TODO: Lab 4.6 == 重载
  return false;
}

bool Level_Iterator::operator!=(const BaseIterator &other) const {
  // TODO: Lab 4.6 != 重载
  return false;
}

BaseIterator::value_type Level_Iterator::operator*() const {
  // TODO: Lab 4.6 * 重载
  return {};
}

BaseIterator::pointer Level_Iterator::operator->() const {
  // TODO: Lab 4.6 -> 重载
  return nullptr;
}
```

类似地, 你可以先看看接下来要实现的一些辅助功能函数, 也许你会在实现这些运算符重载时需要用到它们。

## 2.3 辅助函数
```go
std::pair<size_t, std::string> Level_Iterator::get_min_key_idx() const {
  // TODO: Lab 4.6 获取当前 key 最小的迭代器在 iter_vec 中的索引和具体的 key
  return {};
}

void Level_Iterator::skip_key(const std::string &key) {
  // TODO: Lab 4.6 跳过 key 相同的部分(即被当前激活的迭代器覆盖的写入记录)
}

void Level_Iterator::update_current() const {
  // TODO: Lab 4.6 更新当前值 cached_value
  // ? 实现 -> 时你也许会用到 cached_value
}
```

# 3 测试
次小节的测试将在实现下一小节 [Lab 4.7 复杂查询](./lab4.7-Complicated-Query.md) 后统一进行。