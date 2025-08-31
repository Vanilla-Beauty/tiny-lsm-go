# Lab 1.2 迭代器
# 1 概述
这一部分的内容很简单, 只需要补全跳表迭代器即可。跳表的迭代器基本上就是对`SkiplistNode`的最简化封装, 这一小节的代码量非常少, 也很简单, 不过重点是迭代器的设计和基类的继承关系。

我们先来看它继承了什么基类：
```go
class SkipListIterator : public BaseIterator
```
这里的`BaseIterator`是所有组件的基类, 它的声明在`include/iterator/iterator.h`中。它是后续我们不同组件之间交互的桥梁。建议同学们认真读一下相关代码，很简单但很重要。

# 2 迭代器补全
你需要补全`src/skiplist/skipList.cpp`中标记为`// TODO: Lab1.2`的迭代器函数：
```go

BaseIterator &SkipListIterator::operator++() {
  // TODO: Lab1.2 任务：实现SkipListIterator的++操作符
  return *this;
}

bool SkipListIterator::operator==(const BaseIterator &other) const {
  // TODO: Lab1.2 任务：实现SkipListIterator的==操作符
  return true;
}

bool SkipListIterator::operator!=(const BaseIterator &other) const {
  // TODO: Lab1.2 任务：实现SkipListIterator的!=操作符
  return true;
}

SkipListIterator::value_type SkipListIterator::operator*() const {
  // TODO: Lab1.2 任务：实现SkipListIterator的*操作符
  return {"", ""};
}

IteratorType SkipListIterator::get_type() const {
  // TODO: Lab1.2 任务：实现SkipListIterator的get_type
  // ? 主要是为了熟悉基类的定义和继承关系
  return IteratorType::Undefined;
}
```

# 3 测试
现在你应该能通过`test/test_skiplist.cpp`中的`SkipListTest.Iterator`单元测试了

没问题我们开始`Lab 1`的最后一部分: [Lab 1.3 范围查询](./lab1.3-range-query.md)
