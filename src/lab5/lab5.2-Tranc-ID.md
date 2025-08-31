# Lab 5.2 引入事务 ID
之前我们已经对各个组件的接口进行了统一, 添加了`tranc_id`这个事务id参数, 接下来这个章节, 我们将介绍顶层的事务设计, 即事务id是如何生成的, 实现相关的事务管理器。

# 1 事务的设计思想
我们先给出一个完成后的`demo`, 演示我们的事务设计是如何工作的, 代码如下:
```go
#include "../include/lsm/engine.h"
#include <iostream>
#include <string>

int main() {
  // create lsm instance, data_dir is the directory to store data
  LSM lsm("example_data");

  // put data
  lsm.put("key1", "value1");

  // Query data
  auto value1 = lsm.get("key1");
    std::cout << "key1: " << value1.value() << std::endl;


  // transaction
  auto tranc_hanlder = lsm.begin_tran();
  tranc_hanlder->put("xxx", "yyy");
  tranc_hanlder->put("yyy", "xxx");
  tranc_hanlder->commit();

  auto res = lsm.get("xxx");
  std::cout << "xxx: " << res.value() << std::endl;

  lsm.clear();

  return 0;
}
```
这里我们可以通过一个`begin_tran`函数获取一个事务的处理句柄, 然后通过这个句柄进行增删改查操作, 最后通过`commit`或`abort`函数完成提交事务或终结事务的流程.

现在我们要完成的就是接受`begin_tran`的事务管理器. 这里有一些设计问题我们需要提前明确:

1. `begin_tran`获取的事务句柄肯定会分配一个事务`id`, 那么没有开启事务的`put/get/remove`操作的事务`id`是什么呢?
2. `begin_tran`进行增删改查的操作如何保证不同事务的隔离性?

首先回答第一个问题, 我们可以使用一个全局的`atomic`变量来作为事务id, 这个变量在每次调用`begin_tran`或`put/get/remove`时自增, 这样就可以保证每个事务或单次操作都有一个唯一的`tranc_id`(这里的`tranc_id`和事务`id`是同义词). 换句话说, 普通的`put/get/remove`就是操作数量为1的简单事务。

然后是第二个问题，这实际上取决于我们的事务隔离级别:

1. `Read Uncommitted`: 允许读取未提交的数据, 也就是脏读. 这种情况下, 我们可以将事务的句柄(案例代码中的`tranc_hanlder`)进行增删改查的数据直接写入到`memtable`中, 这样就可以让其他的事务可以从`memtable`中读取到未提交的数据, 速度肯定很快. 但是这里有一个场景需要尤其注意, 就是我们事务`rokkback`(或者是`abort`)时, 必须撤销已经写入到`memtable`的数据, 因此这里需要我们记录事务的操作记录和以前的历史记录, 然后在`abort`时, 将`memtable`中的数据进行回滚.
2. `Read Committed`: 允许读取已提交的数据, 也就是不可脏读. 这种情况下, 我们可以将事务的句柄(案例代码中的`tranc_hanlder`)进行增删改查的数据暂存到句柄的上下文, 因此其他事务从`memtable`中是查不到这个事务未提交的数据的, 但这个事务自身查询时可以从自己的上下文中读取到未提交的数据.
3. `Repeatable Read`: 在`Read Committed`的基础上解决了不可重复读的问题, 也就是在同一个事务中, 多次读取同一数据的结果是一样的. 这种情况下, 我们可以将每次`get`的数据同样暂存到句柄的上下文, 后续查询相同的`key`时, 从上下文中读取到相同的数据.
4. `Serializable`: 这个这个事务隔离级别我们在关系型数据库中是进一步解决`幻读`现象的, 例如: 在`Repeatable Read`隔离级别下，事务A读取了年龄>30的员工，得到10条记录。此时事务B插入了一个年龄31的新员工并提交。事务A再次读取同样的条件，可能会看到11条记录（幻读）。但在`Serializable`隔离级别下，事务B的插入会被阻塞或者事务A的两次读取结果保持一致，避免幻读。在我们的`KV`数据库中, 我们只需要保证事务提交时进程冲突检查、且按照事务`id`的顺序依次提交即可(虽然这样性能很低)。

# 2 事务管理器的设计方案
## 2.1 组件关系设计
还记得我们之前实现的`LSm`和`LSMEngine`吗? 当时我们将`LSMEngine`包裹在`LSM`中, `LSMEngine`中封装了`memtable`, `sst`等组件, 我们却进一步将其封装在`LSM`中, 这样的目的就是在后续中加入其他同级别的组件, 例如本章的事务管理器, `LSM`的定义为:
```go
class LSM {
private:
  std::shared_ptr<LSMEngine> engine;
  std::shared_ptr<TranManager> tran_manager_;

public:
  // ...
};
```
这里我们对`LSMEngine`和`TranManager`都使用了`shared_ptr`进行封装。`LSMEngine`我们之前已经介绍过了，而`TranManager`就是本章我们要实现的事务管理器。

## 2.1 功能1-分配事务 id
首先事务管理器的基础职责之一就是分配事务`id`, 我们看看其中一个`put`接口:
```go
class TranManager : public std::enable_shared_from_this<TranManager> {
public:
  // ...

  uint64_t getNextTransactionId();
  // ...

private:
  mutable std::mutex mutex_;
  std::shared_ptr<LSMEngine> engine_;
  std::shared_ptr<WAL> wal; // 暂时忽略
  std::string data_dir_;
  // std::atomic<bool> flush_thread_running_ = true; // 废弃的设计方案
  std::atomic<uint64_t> nextTransactionId_ = 1;
  std::atomic<uint64_t> max_flushed_tranc_id_ = 0;
  std::atomic<uint64_t> max_finished_tranc_id_ = 0;
  std::map<uint64_t, std::shared_ptr<TranContext>> activeTrans_;
  FileObj tranc_id_file_;
};

void LSM::put(const std::string &key, const std::string &value) {
  auto tranc_id = tran_manager_->getNextTransactionId();
  engine->put(key, value, tranc_id);
}
```

这里顺带补充我们的查询接口的设计, 你可以看到, 在没有开启事务的情况下, 即时是一次简单的`put`操作都会分配一个事务`id`, 因此这里的`tranc_id`是必须的, 其并不独属于我们的事务模块, 只是简单的`put/get/remove`操作数量只有一个而已(或者是一次性的`batch`接口, 总之不会有多步骤的操作)

## 2.2 功能2-分配事务上下文
回顾我们之前的Demo:
```go
auto tranc_hanlder = lsm.begin_tran();
```
这里的`begin_tran`会返回一个事务上下文(或者叫事务句柄也行), 我们可以在这个上下文中进行增删改查操作, 然后通过`commit`或`abort`函数完成提交事务或终结事务的流程. 我们看看这个上下文的定义:
```go
class TranContext {
  friend class TranManager;

public:
  TranContext(uint64_t tranc_id, std::shared_ptr<LSMEngine> engine,
              std::shared_ptr<TranManager> tranManager,
              const enum IsolationLevel &isolation_level);
  void put(const std::string &key, const std::string &value);
  void remove(const std::string &key);
  std::optional<std::string> get(const std::string &key);

  // ! test_fail = true 是测试中手动触发的崩溃
  bool commit(bool test_fail = false);
  bool abort();
  enum IsolationLevel get_isolation_level();

public:
  std::shared_ptr<LSMEngine> engine_;
  std::shared_ptr<TranManager> tranManager_;
  uint64_t tranc_id_;
  std::vector<Record> operations;
  std::unordered_map<std::string, std::string> temp_map_;
  bool isCommited = false;
  bool isAborted = false;
  enum IsolationLevel isolation_level_;

private:
  std::unordered_map<std::string,
                     std::optional<std::pair<std::string, uint64_t>>>
      read_map_;
  std::unordered_map<std::string,
                     std::optional<std::pair<std::string, uint64_t>>>
      rollback_map_;
};
```

可以看到, 事务上下文主要包含以下内容:

1. `tranc_id_`: 事务`id`
2. `engine_`: `LSM`引擎的指针, 需要保证其在自身生命周期内有效
3. `tranManager_`: 事务管理器的指针, 需要保证其在自身生命周期内有效
4. `operations`: 事务操作记录, 也就是后续转化为`WAL`日志的内容
5. `temp_map_`: 事务上下文中的临时数据, 主要是实现事务的隔离性, 例如`Read Committed`和`Repeatable Read`隔离级别下, 我们需要将`get`的数据暂存到这个临时数据中, 避免被其他事务读取到未提交的数据
6. `rollback_map_`: 事务回滚记录, 主要用于事务的回滚

## 2.3 功能3-事务状态的维护
我们继续看我们定义的事务管理器的其他成员:
```go
```go
class TranManager : public std::enable_shared_from_this<TranManager> {
private:
  mutable std::mutex mutex_;
  std::shared_ptr<LSMEngine> engine_;
  std::shared_ptr<WAL> wal; // 暂时忽略
  std::string data_dir_;
  // std::atomic<bool> flush_thread_running_ = true; // 废弃的设计方案
  std::atomic<uint64_t> nextTransactionId_ = 1;
  std::atomic<uint64_t> max_flushed_tranc_id_ = 0;
  std::atomic<uint64_t> max_finished_tranc_id_ = 0;
  std::map<uint64_t, std::shared_ptr<TranContext>> activeTrans_;
  FileObj tranc_id_file_;
};
```

这里我们关注几个`std::atomic<uint64_t`类型的原子变量, 他们的作用和含义解释如下:

1. 负责记录当前事务完成状态:
   1. `max_flushed_tranc_id_`: 最大的已经刷入`sst`中的事务
   2. `max_finished_tranc_id_`: 最大的已经完成的事务(但数据可能还存在于内存中)
   3. `nextTransactionId_`: 下一个分配事务的`id`
2. 负责事务状态的持久化操作

这里特别进行说明, 为什么要负责是事务的持久化操作, 因为我们后续会实现`WAL`(Write Ahead Log), 这个日志会记录每次的操作, 当我们的数据库崩溃后重启时, 会根据`WAL`中的操作记录进行恢复, 这里的操作记录是指`put`、`remove`等操作, 但是我们需要在重启时知道哪些操作是已经完成的, 哪些操作是未完成的, 因此我们需要在`WAL`中记录每个事务的状态, 这个状态就是`max_flushed_tranc_id_`, 因此在重放`WAL`的操作时, 我们需要根据这个状态来判断哪些操作是已经完成的, 哪些操作是未完成的。

# 3 代码实现
这里我们进入今天的主题, 如何实现不同隔离级别下的事务操作。

本小节实验中，你需要修改的代码为：
- `src/lsm/engine.cpp` (Optional, 你也许会自行设计`LSM`类中`TranManager`的初始化逻辑)
- `src/lsm/transation.cpp`
- `include/lsm/transaction.h`

## 3.1 事务上下文的创建和分配
### 3.1.1 事务上下文的构造函数
这里我们从事务上下文的生命周期的历程逐步实现其关键的接口, 首先是构造函数:
```go
TranContext::TranContext(uint64_t tranc_id, std::shared_ptr<LSMEngine> engine,
                         std::shared_ptr<TranManager> tranManager,
                         const enum IsolationLevel &isolation_level) {
  // TODO: Lab 5.2 构造函数初始化
}
```

### 3.1.2 事务上下文的分配
有了`TranContext`的构造函数中, 我们可以在`TranManager::new_tranc`接受外部请求完成事务上下文的分配:
```go
std::shared_ptr<TranContext>
TranManager::new_tranc(const IsolationLevel &isolation_level) {
  // TODO: Lab 5.2 事务上下文分配
  return nullptr;
}
```

介于之前已经进行了详细的理论介绍, 这里就不过多介绍你需要进行哪些元数据的记录操作了。此外，类定义中的成员变量你不一定需要全部使用，你可以按照自己的理解选择性地使用预定义的成员变量，也可以自行添加新的成员变量。

## 3.2 事务上下文的接口
接下来是本小节内容的最重要部分，事务上下文接口的实现。这里不同隔离级别的事务操作实现会有所不同，你需要根据不同的事务隔离级别完成不同的`CRUD`逻辑:

> 以下的接口在实现`WAL`后, 你需要在实现接口时考虑`WAL`的持久化操作, 本实验中你暂时不需要考虑`WAL`的持久化操作。

### 3.2.1 put
```go
void TranContext::put(const std::string &key, const std::string &value) {
  // TODO: Lab 5.2 put 实现
}
```

这里有几个点需要你考虑:
1. 事务的可见性设计:
   1. `put`操作如何实现对其他事务的可见性?
   2. `put`操作如何实现对其他事务的隔离性?
2. 回滚设计
   1. 如果事务最后需要回滚, 如何实现?
   2. 回滚是否需要额外的数据结构?

### 3.2.2 get
```go
void TranContext::remove(const std::string &key) {
  // TODO: Lab 5.2 remove 实现
}
```
由于`remove`本质上也是`put`, 因此这里的逻辑和`put`类似, 这里就不做过多解释了。

### 3.2.3 get
```go
std::optional<std::string> TranContext::get(const std::string &key) {
  // TODO: Lab 5.2 get 实现
  return {};
}
```

这里需要考虑:

1. 如果是`Read UnCommitted`隔离级别, 需要考虑如何读取到最新的修改记录
2. 如果是`Read Committed`隔离级别, 需要考虑如何避免读取到未提交的数据
3. 如果是`Repeatable Read`隔离级别, 需要考虑如何避免不可重复读现象

### 3.2.4 commit
```go
bool TranContext::commit(bool test_fail) {
  // TODO: Lab 5.2 commit 实现
  return true;
}
```

`commit`函数应该是这里最复杂的, 这里的重点就是实现事务提交时的冲突检测, 如果检测无冲突且`WAL`持久化成功(后续`Lab`的内容), 返回`true`表示成功提交, 否则返回`false`表示提交失败。

本实验的设计采用了类似`乐观锁`的思想, 所有事务的更新记录只有在提交时才会进行冲突检测, 其逻辑为:

1. 如果隔离级别是`READ_UNCOMMITTED`, 因为之前就已经将更改的数据写入了`MemTable`, 现在只需要直接写入`wal`一个`Commit`记录(目前不涉及, 可先跳过)
2. 如果隔离级别是`REPEATABLE_READ`或`SERIALIZABLE`, 需要遍历所有的操作记录, 判断是否存在冲突, 如果存在冲突则终止事务, 否则将所有的操作记录写入`wal`中, 然后将数据应用到数据库中
3. 完成事务数据同步到`MemTable`后, 更新`max_finished_tranc_id_`并持久化数据

> 这里需要注意的是, 你在进行冲突检测时, `MemTable`和`SST`部分此时应该是不允许写入的, 否则存在并发冲突。这里你的加锁行为可能是类似侵入式的做法（即手动对其他类的内部成员变量进行加锁）
> 
> 同样地, 这里设计`WAL`的部分可以先跳过

### 3.2.5 abort
`abort` 方法用于回滚事务，具体的回滚逻辑取决于你之前对`put`函数的设计:
```go
bool TranContext::abort() {
  // TODO: Lab 5.2 abort 实现
  return true;
}
```
这里同样用`true`表示成功回滚, 否则返回`false`表示回滚失败。

> 在`commit`函数的冲突检测失败后, 也需要进行回滚操作, 不过其回滚是被动的
>
> 而`abort`函数是`client`主动发起的回滚操作


## 3.3 事务状态的维护
之前提到过, `TranManager`中定义了几个`std::atomic<uint64_t>`类型的原子变量, 这些变量用于记录事务的状态, 在事务的提交和回滚时, 需要更新这些变量的值, 以便在重启时进行恢复:
```go
void TranManager::write_tranc_id_file() {
  // TODO: Lab 5.2 持久化事务状态信息
}

void TranManager::read_tranc_id_file() {
  // TODO: Lab 5.2 读取持久化的事务状态信息
}

void TranManager::update_max_finished_tranc_id(uint64_t tranc_id) {
  // TODO: Lab 5.2 更新持久化的事务状态信息
}

void TranManager::update_max_flushed_tranc_id(uint64_t tranc_id) {
  // TODO: Lab 5.2 更新持久化的事务状态信息
}
```
> 在你操作原子变量时可以不使用锁而实现并发安全性, 不过你需要了解[**内存顺序**](https://en.cppreference.com/w/cpp/atomic/memory_order)的概念

完成上面的持久化操作后, 你在存储引擎启动后也需要从持久化的文件中恢复这些元信息:
```go
TranManager::TranManager(std::string data_dir) : data_dir_(data_dir) {
  auto file_path = get_tranc_id_file_path();

  // TODO: Lab 5.2 初始化时读取持久化的事务状态信息
}
```
试想, 你在第一次存储要求启动时进行了若干操作, 事务`id`已经分配到了100, 而之后你重启了数据库, 此时事务`id`如果又从0开始分配, 而不是从100开始分配, 会发生什么问题?

# 4 测试
现在除了崩溃恢复的部分外, 你应该可以通过所有的`test_lsm`的测试:
```bash
✗ xmake
[100%]: build ok, spent 0.595s
✗ xmake run test_lsm
[==========] Running 9 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 9 tests from LSMTest
[ RUN      ] LSMTest.BasicOperations
[       OK ] LSMTest.BasicOperations (10 ms)
[ RUN      ] LSMTest.Persistence
[       OK ] LSMTest.Persistence (1600 ms)
[ RUN      ] LSMTest.LargeScaleOperations
[       OK ] LSMTest.LargeScaleOperations (17 ms)
[ RUN      ] LSMTest.MixedOperations
[       OK ] LSMTest.MixedOperations (8 ms)
[ RUN      ] LSMTest.IteratorOperations
[       OK ] LSMTest.IteratorOperations (9 ms)
[ RUN      ] LSMTest.MonotonyPredicate
[       OK ] LSMTest.MonotonyPredicate (17 ms)
[ RUN      ] LSMTest.TrancIdTest
[       OK ] LSMTest.TrancIdTest (8 ms)
[ RUN      ] LSMTest.TranContextTest
[       OK ] LSMTest.TranContextTest (8 ms)
[ RUN      ] LSMTest.Recover
unknown file: Failure
C++ exception with description "bad optional access" thrown in the test body.
```