# Lab 4.4 TwoMergeIterator
# 1 概述
根据上一节[Lab 4.3 ConcactIterator](./lab4.3-ConcactIterator.md)中对`TwoMergeIterator`的介绍, `TwoMergeIterator`就是整合两个优先级不同的迭代器, 生成一个新的迭代器, 该迭代器优先级为两个迭代器中优先级较高的那个。

其余逻辑上一节[Lab 4.3 ConcactIterator](./lab4.3-ConcactIterator.md)已经有所介绍, 这里不再赘述, 直接开始看头文件定义:
```go
class TwoMergeIterator : public BaseIterator {
private:
  std::shared_ptr<BaseIterator> it_a;
  std::shared_ptr<BaseIterator> it_b;
  bool choose_a = false;
  mutable std::shared_ptr<value_type> current; // 用于存储当前元素
  uint64_t max_tranc_id_ = 0;
};
```
这里也很简单, 就只是存储两个迭代器 的指针`it_a`和`it_b`, 以及一个`choose_a`用于标记当前是否选择了优先级较高的`it_a`迭代器是, 一个`current`用于缓存当前迭代器位置的键值对, 一个`max_tranc_id_`用于记录当前可见事务的最大`id`。

# 2 代码实现
## 2.1 运算符重载
```go
BaseIterator &TwoMergeIterator::operator++() {
  // TODO: Lab 4.4: 实现 ++ 重载
}

bool TwoMergeIterator::operator==(const BaseIterator &other) const {
  // TODO: Lab 4.4: 实现 == 重载
  return false;
}

bool TwoMergeIterator::operator!=(const BaseIterator &other) const {
  // TODO: Lab 4.4: 实现 != 重载
  return false;
}

BaseIterator::value_type TwoMergeIterator::operator*() const {
  // TODO: Lab 4.4: 实现 * 重载
  return {};
}

TwoMergeIterator::pointer TwoMergeIterator::operator->() const {
  // TODO: Lab 4.4: 实现 -> 重载
  return nullptr;
}
```
老套路, 你需要先实现迭代器的各个运算符重载函数, 不过建议你看看下面的辅助函数, 你在实现这些运算符重载函数的时候会用到这些辅助函数, 不妨一起实现了。

## 2.2 辅助函数
首先`choose_it_a`判断当前解引用应该使用哪个迭代器:
```go
bool TwoMergeIterator::choose_it_a() {
  // TODO: Lab 4.4: 实现选择迭代器的逻辑
  return false;
}
```

然后是更新当前缓存值的函数:
```go
void TwoMergeIterator::update_current() const {
  // TODO: Lab 4.4: 实现更新缓存键值对的辅助函数
}
```
这个函数你可能会在之前的自增运算符和`->`运算符重载中调用

最后这个函数是根据事务可见性进行滤除的辅助函数, 当前不需要实现, 只是标记下便于你之后的`Lab`来更新:
```go
void TwoMergeIterator::skip_by_tranc_id() {
  // TODO: Lab xx
}
```

# 3 测试
本小节没有测试, 你完成后续迭代器和涉及迭代器的查询操作后有统一的单元测试。
