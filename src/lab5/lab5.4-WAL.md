# Lab 5.4 WAL 运行机制
上一小节的`Lab`你已经实现了单条`WAL`记录`Record`的设计, 这一小节我们将整合`Record`, 完成`WAL`组件的设计。

# 1 WAL 文件设计
首先，`WAL`文件的内容本质上分就是`Record`的数组。但这里却不仅仅是对`Record`的简单存储，而是需要考虑`WAL`文件的时效性对其进行清理, 以及写入文件的方式。设计要点包括：

1. 刷盘的高效性
   - 我们都知道，当一个事务完成时，必须保证其对应的`WAL`记录被写入磁盘，否则在系统崩溃时，事务的修改将无法恢复。因此，`WAL`记录的写入必须保证原子性。但保证原子性的开销是什么呢? 你需要保证你的`WAL`组件写入磁盘时的效率(例如设置缓冲区, 或者是异步刷盘)
2. 过时`WAL`记录的清理
   - 事务操作的记录都会记录到`WAL`文件中进行持久化, 但其本身对数据库的操作也会随着刷盘形成`SST`完成真正的持久化, 此时之前的`WAL`记录已经不再需要, 需要被清理。因此，`WAL`文件需要有一个机制来清理过时的`WAL`记录。

# 2 WAL 组件的设计思路
老规矩, 我们先看看`WAL`组件的定义:
```go
// WAL manages write-ahead log files
type WAL struct {
	config      *Config
	mu          sync.Mutex
	buffer      []*Record
	currentFile *os.File
	currentSeq  int64
}
```

这里对其成员变量进行简单解释：
1. `config`: 配置项，配置了清理时间间隔、日志文件位置、缓冲区大小、文件阈值等
2. `mu`: 锁，并发控制，当然取决于你的实现是否使用
3. `buffer`: 缓冲区，用于存储待写入的日志记录，当然你也可以使用无缓冲的实现
4. `currentFile`: 当前日志文件，用于写入日志记录
5. `currentSeq`: 当前日志文件序列号，用于生成日志文件名

这里重点介绍下为什么会设计`currentSeq`这个概念，原因是为了方便清理工作，在某一时刻，你的`WAL`文件有如下：
```text
wal.0
wal.1
wal.2
```

`wal.0`是你最开始的日志文件，且此时`currentSeq == 0`。当其容量达到阈值后，该文件呗冻结，新建`wal.1`，并将`currentFile`指向新的`wal.1`，同时将`currentSeq`加1，换言之，`currentSeq`就是当前文件的序列号。

为什么如此做呢？因为这样清理线程很方便做清理工作，这里的清理线程实现很简易（你马上就需要实现），其就是简单`Sleep`一段时间，然后检查`WAL`文件中的事务是否都被持久化了，如果是则删除这个`WAL`文件。此时，清理线程可以按照`WAL`文件的后缀从小大大进行遍历：
1. 记当前`WAL`文件的后缀序列化为`x`
2. 如果`WAL.x`中的事务都已经被持久化：
   1. 删除`WAL.x`文件
   2. 自增`x`, 继续遍历
3. 如果`WAL.x`中的事务没有被持久化：
   1. 停止此次遍历检查，因为既然序列号x的日志文件的事务都没有被持久化，那么之后的日志文件也不可能被持久化

按照这样的实现，清理线程的效率就很高了。

# 3 代码实现
- `pkg/wal/wal.go`
- `pkg/lsm/txn_engine.go`

## 3.1 初始化
```go
// pkg/wal/wal.go
// New creates a new WAL instance
func New(config *Config, checkpointTxnID uint64) (*WAL, error) {
	// TODO: Lab 5.4

	return nil, nil
}
```
`New`函数创建一个WAL实例，但其不仅仅是填充结构体参数，你需要完成如下工作：
1. 判断配置指定的文件夹是否为空
  1. 如果文件夹为空，则创建一个WAL文件夹
  2. 否则需要找出文件夹中最大的日志文件序列号，初始化变量`currentSeq`
2. 根据`currentSeq`创建一个日志文件`currentFile`(如果之前没有旧数据，则`currentSeq == 0`)

## 3.2 关闭
```go
// pkg/wal/wal.go
// Close closes the WAL and stops background tasks
func (w *WAL) Close() error {
	// TODO: Lab 5.4
	return nil
}
```
`Close`函数需要完成如下工作：
1. 停止后台任务
2. 清空缓冲区
3. 持久化当前的日志文件后关闭

## 3.3 插入WAL记录
```go
// pkg/wal/wal.go
// Log adds records to the WAL
func (w *WAL) Log(records []*Record, forceFlush bool) error {
	// TODO: Lab 5.4
	return nil
}
```
`Log`函数就是将你之前实现的`Record`写入。`WAL`日志文件中。当然, 你可以选择是直接写入文件中, 还是先写入我们自己控制的缓冲区中

## 3.4 读取WAL记录
```go
// Recover reads and returns all records from WAL files that are after the checkpoint
func Recover(logDir string, checkpointTxnID uint64) (map[uint64][]*Record, error) {
	// TODO: Lab 5.4

	return nil, nil
}
```
注意，这个函数返回的是一个`map`，这个`map`的`key`是事务ID，`value`是事务中的所有记录。这个名字其实有点歧义，其仅仅是读取所有的WAL记录，并按照事务ID进行整理，并不会做实际回放，因为这些事务不一定都需要进行回放。下一章你实现的崩溃恢复进行重放时，需要调用这个函数，然后根据事务是否需要重放进行进一步操作。

## 3.5 一些可选的函数
如果你实现的策略使用了缓冲区，建议你完成下面这个刷盘函数：
```go
// Flush forces all buffered records to be written to disk
func (w *WAL) Flush() error {
	// TODO Lab 5.4

	return nil
}
```
在使用了缓冲区的前提下，你在`Close`时一定要记得刷出缓冲区的内容

# 4 WAL 清理
由于我们的数据库只要开启了事务，`WAL`文件会持续增长，因此我们单独开启了一个线程来清理旧的`WAL`文件。其启动逻辑是：
```go
func (e *Engine) startBackgroundWorkers() {
	// Flush worker
	e.wg.Add(3)
	go e.flushWorker()
	go e.cleanWalWorker() // This one for wal cleanup
	go e.syncTxnStatusWorker()

	if e.config.Compaction.EnableAutoCompaction {
		e.wg.Add(1)
		go e.compactionWorker()
	}
}

// cleanupLoop runs in a background goroutine to clean old WAL files
func (e *Engine) cleanWalWorker() {
	defer e.wg.Done()
	logger.Infof("Starting WAL cleanup loop\n")

	ticker := time.NewTicker(time.Duration(e.config.WAL.CleanInterval))
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			activeTxnIDs := e.txnManager.GetactiveTxnIDs()
			e.wal.CleanOldFiles(activeTxnIDs)
		}
	}
}
```
`cleanWalWorker`是一个简单的清理线程，会定时检查WAL文件，并清理过期的WAL文件。你需要实现的就是这个`CleanOldFiles`函数：
```go
// CleanOldFiles removes WAL files that contain only committed transactions
func (w *WAL) CleanOldFiles(activeTxnIDs map[uint64]struct{}) {
	// TODO: Lab 5.4
}
```
这里清理的逻辑之前都讲过了，这里解释下这个函数的参数，因为我们是将`WAL`单独作为一个模块，其与事务管理模块是分离开的，因此其是不知道哪些事务事务提交了但没有刷盘，因此这里需要由上层进行判断并将这些信息提供给`CleanOldFiles`函数。

# 5 Transaction 逻辑更新
之前你实现的`PutWithTxn`, `GetWithTxn`等函数中, 你的实现仅仅是将操作记录记录在了`operations`数组中(甚至没有记录, 因为那时你可能不知道这个成员变量是做什么的)。

现在你已经实现的`WAL`的刷盘接口, 因此你需要更新`TranContext`的这些函数, 使其能够将操作记录写入`WAL`文件中。不过这里你需要尤其注意冲突检测的问题, 不同的策略的冲突检测实现难度大不相同

- `commit`时统一进行冲突检测并写入`WAL`文件, 这种方式实现最简单, 但性能较差
- `put`, `get`, `remove`时进行就分批写入`WAL`文件, 换句话说你需要利用之前的缓冲区, 这种方式实现需要你在从图检测时需要考虑`WAL`文件中的记录的有效性控制, 实现难度较大, 但性能较好

你在更新`TranContext`的`PutWithTxn`, `GetWithTxn`等函数中, 下面这个辅助函数也许对你有用:
```go
bool TranManager::write_to_wal(const std::vector<Record> &records) {
  // TODO: Lab 5.4

  return true;
}
```

# 5 测试
`WAL`组件的测试代码在`pkg/wal/wal_test.go`中, 你需要保证你的`WAL`组件能够通过这些测试, 但这个测试文件编写其实非常粗糙, 因为本节`Lab`对你的实现方案没有做任何限制, 因此你的实现的元数据也不好测试。因此, 这个测试看看就行, 在你完成下一小节(也是本章最后一个`Lab`)的逻辑后, 你可以通过`pkg/lsm`包下的所有测试。