# Lab 5.5 崩溃恢复

# 1 崩溃恢复运行机制
当某一时刻存储引擎发送崩溃时, 需要进行崩溃恢复，以恢复数据状态。崩溃恢复的关键是**回放日志**，以确定已提交的事务，并执行其操作。在这个描述中我们不难得到一个信息, 即成功执行的事务一定要先将操作记录持久化到日志中, 然后再在内存中进行操作, 最后返回成功信息给客户端或者调用者。这也是为什么这个机制称为**预写式日志**。其崩溃恢复的工作流程包括：

1. **日志回放**：从最后一个检查点开始扫描日志，按顺序处理所有已提交事务（`COMMIT`标记后的操作）。
2. **Redo阶段**：重新执行所有已提交事务的`PUT/REMOVE`操作，覆盖当前数据状态。
3. **Undo阶段（可选）**：若事务未提交（无`COMMIT`标记），则直接丢弃其操作记录。

**示例场景**
假设事务`TX100`依次执行`PUT key1=value1`和`REMOVE key2`，其日志内容如下：
```plaintext
BEGIN TX100
PUT key1 5 value1
DELETE key2
COMMIT TX100
```

事务`TX100`在调用`commit`函数后, 需要将上述日志刷入`wal`文件完成预写这一步骤后, 才会返回`client`事务提交成功。一开始提交的事务，其数据一定是只存在于`MemTable`中的, 此时如果存储引擎崩溃, 刚刚完成的事务是没法刷入到`sst`文件的, 但我们重启时可以检查`wal`文件的内容, 将其与数据库持久化的状态进行比对(持久化的状态包括最大已完成的事务id、最大已经刷盘的事务id， 见上一章的内容)，如果其事务`id`比目前已经持久化到`sst`的最大事务`id`大，则说明该事务需要进行重放, 另一方面, 如果事务记录的最后一个条目是`Rollback`，则说明该事务需要被回滚, 则不需要在崩溃恢复时进行重放.

将上述流程总结如下:

**情形1: 正常提交事务、SST刷盘正常**
1. 事务开始, 写入`BEGIN`标记到`WAL`日志中。(此时的`WAL`日志可能存在于缓冲区, 没有刷入文件)
2. 执行若干`PUT/DELETE`操作, 将操作记录写入`WAL`日志中。(此时的`WAL`日志可能存在于缓冲区, 没有刷入文件)
   1. 如果隔离级别是`Read Uncommitted`, 可以将`PUT/DELETE`操作直接应用到数据库
   2. 其他隔离级别则将`PUT/DELETE`操作暂存到事务管理的上下文内存中, 等待事务提交时再应用到数据库
3. 提交事务:
   1. 将`COMMIT`标记写入`WAL`日志中。(此时的`WAL`日志可能存在于缓冲区, 没有刷入文件)
   2. 将`WAL`日志刷入磁盘。(此时`WAL`日志已经刷入磁盘)
   3. 如果隔离级别不是`Read Uncommitted`, 则将暂存的`PUT/DELETE`操作应用到数据库
   4. 返回给`client`成功或失败
4. 之前事务的`PUT/DELETE`操作的变化应用到数据库仍然是位于`MemTable`中的, 其会稍后输入`SST`

**情形2: 正常提交事务、SST刷盘崩溃**
1. 事务开始, 写入`BEGIN`标记到`WAL`日志中。(此时的`WAL`日志可能存在于缓冲区, 没有刷入文件)
2. 执行若干`PUT/DELETE`操作, 将操作记录写入`WAL`日志中。(此时的`WAL`日志可能存在于缓冲区, 没有刷入文件)
   1. 如果隔离级别是`Read Uncommitted`, 可以将`PUT/DELETE`操作直接应用到数据库
   2. 其他隔离级别则将`PUT/DELETE`操作暂存到事务管理的上下文内存中, 等待事务提交时再应用到数据库
3. 提交事务:
   1. 将`COMMIT`标记写入`WAL`日志中。(此时的`WAL`日志可能存在于缓冲区, 没有刷入文件)
   2. 将`WAL`日志刷入磁盘。(此时`WAL`日志已经刷入磁盘)
   3. 如果隔离级别不是`Read Uncommitted`, 则将暂存的`PUT/DELETE`操作应用到数据库
   4. 返回给`client`成功或失败
4. 之前事务的`PUT/DELETE`操作的变化应用到数据库仍然是位于`MemTable`中的, 其稍后刷入`SST`奔溃
5. 数据库重启后执行崩溃回复
   1. 检查`WAL`文件的记录
   2. 整合事务`id`每条记录, 忽略以`Rollback`结尾的事务
   3. 若事务以`Commit`结尾, 则将事务`id`与已经刷盘的`SST`中的最大事务`id`进行比对
      1. 若事务`id`大于`SST`的最大事务`id`, 执行重放操作
      2. 若事务`id`小于`SST`的最大事务`id`, 则忽略该事务, 因为其已经被数据库执行过了

**情形3: 事务回滚**
1. 事务开始, 写入`BEGIN`标记到`WAL`日志中。(此时的`WAL`日志可能存在于缓冲区, 没有刷入文件)
2. 执行若干`PUT/DELETE`操作, 将操作记录写入`WAL`日志中。(此时的`WAL`日志可能存在于缓冲区, 没有刷入文件)
   1. 如果隔离级别是`Read Uncommitted`, 可以将`PUT/DELETE`操作直接应用到数据库
   2. 其他隔离级别则将`PUT/DELETE`操作暂存到事务管理的上下文内存中, 等待事务提交时再应用到数据库
3. 回滚事务:
   1. 将`Rollback`标记写入`WAL`日志中。(此时的`WAL`日志可能存在于缓冲区, 没有刷入文件)
   2. 将`WAL`日志刷入磁盘。(此时`WAL`日志已经刷入磁盘)
   3. 如果隔离级别不是`Read Uncommitted`, 则将暂存的`PUT/DELETE`操作简单丢弃即可
   4. 如果隔离级别是`Read Uncommitted`, 则将操作前的数据库状态进行还原(作者的设计是利用`TranContext`中的`rollback_map_`进行还原, 当然这取决于你之前的`Lab`实现)
   5. 返回给`client`成功或失败

# 2 崩溃恢复代码实现
本小节实验, 你需要更改的代码文件包括:
- `pkg/lsm/engine.go`

## 2.1 recoverFromWAL

这里的崩溃恢复需要在引擎启动时进行判断, 因此其与不同组件的构造函数息息相关, 由于这里不同组件的耦合程度较高, 故先统一介绍这流程:
```text
-> LSM::LSM 构造函数启动
   -> 调用 LSMEngine 的构造函数
   -> 调用 TranManager 的构造函数
      -> TranManager 初始化除了 WAL 之外的组件
   -> 调用 TranManager::check_recover 检查是否需要重放WAL日志
   -> 将重放的 WAL 日志应用到 LSMEngine
   -> 调用 TranManager::init_new_wal 初始化 WAL 组件
-> LSM::LSM 的其他逻辑...
-> LSM::LSM 构造函数结束
```

```go
// pkg/lsm/engine.go
// recoverFromWAL recovers uncommitted transactions from WAL logs
func (e *Engine) recoverFromWAL() error {
	// TODO: Lab 5.5

	return nil
}
```
`recoverFromWAL`这个函数需要你在启动整个存储引擎时进行调用，其功能是检测`WAL`文件中是否有需要进行重复的日志，举个例子:
```text
T1 ctx1 running, ctx2 running
T2 ctx1 commit, ctx2 running
T3 crash, both data from in ctx1 and ctx2 are not flushed to sst (in `MemTable`)
T4 recover
```
在这个例子中, `ctx1`在崩溃前成功提交但数据没有刷入`SST`, 存在与内存的`MemTable`中; `ctx2`在崩溃时仍未提交, 因此在`T4`进行崩溃恢复时, 属于`ctx1`的`WAL`日志需要进行重放, 而属于`ctx2`的`WAL`日志则不需要进行重放(因为其根本没有提交)

`WAL::recover`函数就是整理需要重放的`WAL`日志, 返回一个`map`, 其中`key`为事务`id`, `value`为该事务的所有`WAL`操作记录

`recoverFromWAL`的调用位置在这里：
```go
// pkg/lsm/engine.go
// recover recovers the engine state from disk
func (e *Engine) recover() error {
	// First, recover SST files
	if err := e.levels.LoadExistingSSTs(); err != nil {
		return fmt.Errorf("failed to load existing SST files: %w", err)
	}

	// Then, recover from WAL
	if e.wal != nil {
		if err := e.recoverFromWAL(); err != nil {
			return fmt.Errorf("failed to recover from WAL: %w", err)
		}
	}

	return nil
}
```

那么问题来了，什么情况下我们的`WAL`日志是需要重放的？
**答案是已经提交但m没有刷到SST文件中的数据**，换言之，存储引擎崩溃时这些数据存在于内存的`MemTable`中。因此，你实现的事务管理器需要意识到什么时候，某个事务提交，以及什么时候这个事务被持久化到`SST`了。这个时候，你就意识到你在`Lab-5.2`中实现的事务管理器的重要性了。


## 2.2 replayTransaction
```go
// replayTransaction replays a single transaction from WAL records
func (e *Engine) replayTransaction(txnID uint64, records []*wal.Record) error {
	// TODO: Lab 5.5
	return nil
}
```
`replayTransaction`这个函数顾名思义，就是在判断该事务需要进行重放时，调用存储引擎的接口进行重放操作。其会被`recoverFromWAL`函数调用。


## 2.3 初始化后的操作
在重放完成后，需要重新初始化 WAL，以便后续事务的日志记录：
```go
void TranManager::init_new_wal() {
  // TODO: Lab 5.5 初始化 wal
}
```


# 3 测试
现在你应该可以通过`pkg/lsm/txn_test.go`中所有的测试。