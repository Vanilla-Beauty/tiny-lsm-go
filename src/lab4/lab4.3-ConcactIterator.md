# Lab 4.3 ConcactIterator
# 1 概述
其实这一章出现在这里是有一点突兀的, 照理说我们正常实现基础`LSMEngine`的下一个步骤是`Compact`。讲到这里我们接得知道`Compact`的逻辑了。

这里用一个具体案例进行讲解。

我们假设此时的`SST`状态是这样的:
```bash
Level 0: sst_15(key000-key050), sst_14(key005-key030), sst_13(key020-key040)
Level 1: sst_10(key100-key120), sst_11(key121-key140), sst_12(key141-key160)
Level 2: sst_08(key100-key120), sst_09(key121-key140)
```
我们对每一层的`SST`需要进行限制, 当这一层的`SST`数量超过阈值时, 我们需要将这一层所有的`SST`进行`Compact`到下一层。

在这个案例中, 我们下一次`flush`刷盘后, 假设得到了一个`Level 0`的`SST`为`sst_16(key060-key080)`, 那么此时`Level 0`的`SST`数量超过了阈值4(这个是是假定的, 实际上是可在`config.toml`中配置的), 我们需要将`Level 0`的`SST`进行`sst_16, sst_15, sst_14, sst_13`到`Level 1`。但我们新`Compact`的`SST`会和`Level 1`本来的`SST`的`key`存在范围重叠, 这是不允许的, 所以实际上, 我们是将原来的`Level 0`和旧的`Level 1`的所有`SST`一并进行重新整理`Compact`形成新的`Level 1 SST`, 这里实际上就是用迭代器将2个`Level`的迭代器进行串联, 遍历这个迭代器, 逐一构建新的`Level 1`的`SST`, 那么是怎么个串联法呢?

1. 首先, `Level 0`的`SST`之间存在`key`的重叠, 需要进行去重, 这里我们可以复用已有的`HeapIterator`, 见[Lab 2.2 迭代器](../lab2/lab2.2-iterator.md)
2. 其次就是我们将要实现的`ConcactIterator`, 这个迭代器会串联`Level 1`的所有`SST`形成层间迭代器
3. 最后就是之后小节实现的`TwoMergeIterator`, 这个迭代器会串联`HeapIterator`和`ConcactIterator`, 按照优先级对迭代器进行输出, 遍历`TwoMergeIterator`即可构建新的`Level 1`的`SST`

> **Question**
> 
> 如果`Compact`发生的`Level`不是`Level 0`和`Level 1`, 而是`Level x`和`Level y`(`x>0`)呢? 
>
> 答案显而易见, 还是用`TwoMergeIterator`对2个`Level`的层间迭代器进行遍历, 遍历过程中构造新的`Level y`的`SST`。只是`TwoMergeIterator`中包裹的`Level x`的迭代器从`HeapIterator`变成了`ConcactIterator`。

这一小节我们先实现`ConcactIterator`。

# 2 代码实现
你需要修改的文件:
- `src/sst/concact_iterator.cpp`
- `include/sst/concact_iterator.h` (Optional)
## 2.1 头文件分析
按照惯例, 我们简单分析下头文件定义:
```go
class ConcactIterator : public BaseIterator {
private:
  SstIterator cur_iter;
  size_t cur_idx; // 不是真实的sst_id, 而是在需要连接的sst数组中的索引
  std::vector<std::shared_ptr<SST>> ssts;
  uint64_t max_tranc_id_;
};
```
这里的`ssts`就是这一层所有`SST`句柄的数组, `cur_idx`是当前迭代器指向的`SST`在`ssts`中的索引, `cur_iter`是当前`cur_idx`指向的`SST`的迭代器。

`max_tranc_id_`是调用这个迭代器的事务`id`, 也就是其最大的事务可见范围, 现在你不需要实现。

## 2.2 ConcactIterator 实现
这一章由于我们先介绍了`Compact`过程中的逻辑, 因此就先实现最简单的一个`ConcactIterator`:
```go
BaseIterator &ConcactIterator::operator++() {
  // TODO: Lab 4.3 自增运算符重载
  return *this;
}

bool ConcactIterator::operator==(const BaseIterator &other) const {
  // TODO: Lab 4.3 比较运算符重载
  return false;
}

bool ConcactIterator::operator!=(const BaseIterator &other) const {
  // TODO: Lab 4.3 比较运算符重载
  return false;
}

ConcactIterator::value_type ConcactIterator::operator*() const {
  // TODO: Lab 4.3 解引用运算符重载
  return value_type();
}
ConcactIterator::pointer ConcactIterator::operator->() const {
  // TODO: Lab 4.3 ->运算符重载
  return nullptr;
}
```
这里基本上都是实现迭代器的运算符重载函数, 算是比较简单的一个小`Lab`

# 3 测试
本小节没有测试, 你完成后续迭代器和涉及迭代器的查询操作后有统一的单元测试。
