# Lab 5.2 引入事务 ID
之前我们已经对各个组件的接口进行了统一, 添加了`tranc_id`这个事务id参数, 接下来这个章节, 我们将介绍顶层的事务设计, 即事务id是如何生成的, 实现相关的事务管理器。

# 1 事务的设计思想
我们先给出一个单元测试, 演示我们的事务设计是如何工作的, 代码如下:
```go

func TestBasicTransactionOperations(t *testing.T) {
	txnEngine, cleanup := setupTestTxnEngine(t)
	txnEngine.flushAndCompactByHand = true
	defer cleanup()

	// Begin transaction
	txn, err := txnEngine.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	if txn.IsolationLevel() != ReadCommitted {
		t.Errorf("Expected isolation level %v, got %v", ReadCommitted, txn.IsolationLevel())
	}

	// Write data in transaction
	key := "test_key"
	value := "test_value"
	err = txnEngine.PutWithTxn(txn, key, value)
	if err != nil {
		t.Fatalf("Failed to put in transaction: %v", err)
	}

	// Read data in transaction
	readValue, found, err := txnEngine.GetWithTxn(txn, key)
	if err != nil {
		t.Fatalf("Failed to get in transaction: %v", err)
	}

	if !found {
		t.Errorf("Expected to find key %s in transaction", key)
	}

	if readValue != value {
		t.Errorf("Expected value %s, got %s", value, readValue)
	}

	// Commit transaction
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}
```
这里我们可以通过一个`Begin()`函数获取一个事务的处理句柄, 然后通过这个句柄进行增删改查操作, 携带这个句柄的函数形如`xxxWithTxn`, 最后通过`Commit()`或`Rollback()`函数完成提交事务或终结事务的流程.

现在我们要完成的就是接受`Begin()`的事务管理器. 这里有一些设计问题我们需要提前明确:

1. `Begin()`获取的事务句柄肯定会分配一个事务`id`, 那么没有开启事务的`put/get/remove`操作的事务`id`是什么呢?
2. `Begin()`进行增删改查的操作如何保证不同事务的隔离性?

首先回答第一个问题, 我们可以使用一个全局的`atomic`变量来作为事务id, 这个变量在每次调用`Begin()`或`put/get/remove`时自增, 这样就可以保证每个事务或单次操作都有一个唯一的`tranc_id`(这里的`tranc_id`和事务`id`是同义词). 换句话说, 普通的`put/get/remove`就是操作数量为1的简单事务。

然后是第二个问题，这实际上取决于我们的事务隔离级别:

1. `Read Uncommitted`: 允许读取未提交的数据, 也就是脏读. 这种情况下, 我们可以将事务的句柄(案例代码中的`tranc_hanlder`)进行增删改查的数据直接写入到`memtable`中, 这样就可以让其他的事务可以从`memtable`中读取到未提交的数据, 速度肯定很快. 但是这里有一个场景需要尤其注意, 就是我们事务`rokkback`(或者是`abort`)时, 必须撤销已经写入到`memtable`的数据, 因此这里需要我们记录事务的操作记录和以前的历史记录, 然后在`abort`时, 将`memtable`中的数据进行回滚.
2. `Read Committed`: 允许读取已提交的数据, 也就是不可脏读. 这种情况下, 我们可以将事务的句柄(案例代码中的`tranc_hanlder`)进行增删改查的数据暂存到句柄的上下文, 因此其他事务从`memtable`中是查不到这个事务未提交的数据的, 但这个事务自身查询时可以从自己的上下文中读取到未提交的数据.
3. `Repeatable Read`: 在`Read Committed`的基础上解决了不可重复读的问题, 也就是在同一个事务中, 多次读取同一数据的结果是一样的. 这种情况下, 我们可以将每次`get`的数据同样暂存到句柄的上下文, 后续查询相同的`key`时, 从上下文中读取到相同的数据.
4. `Serializable`: 这个这个事务隔离级别我们在关系型数据库中是进一步解决`幻读`现象的, 例如: 在`Repeatable Read`隔离级别下，事务A读取了年龄>30的员工，得到10条记录。此时事务B插入了一个年龄31的新员工并提交。事务A再次读取同样的条件，可能会看到11条记录（幻读）。但在`Serializable`隔离级别下，事务B的插入会被阻塞或者事务A的两次读取结果保持一致，避免幻读。在我们的`KV`数据库中, 我们只需要保证事务提交时进程冲突检查、且按照事务`id`的顺序依次提交即可(虽然这样性能很低)。

# 2 事务管理器的设计方案
## 2.1 组件设计思路
本章的事务管理器, `TransactionManager`的定义为:
```go
// TransactionManager manages database transactions
type TransactionManager struct {
	engine        *Engine
	activeTxns    map[uint64]*Transaction // Transaction 后续马上会讲到
	committedTxns map[uint64]*Transaction
	mu            sync.RWMutex
	config        *TransactionConfig
}
```
可以看到，这里我们获取了一个`Engine`实例指针，这是我们早就熟悉的结构体，用于获取数据库操作。

这里其他几个变量很重要：
1. `activeTxns`：保存所有正在执行的事务。
2. `committedTxns`：保存所有已经提交但很没有刷盘的事务。

通过以上2个`map`, 我们就可以完全描述某一时刻事务的状态，我们的事务一共有如下3种状态：
1. 仍然活跃的事务，其保存在`activeTxns`中。
2. 已经提交但还没有刷盘的事务，其保存在`committedTxns`中。
3. 已经提交并已经刷盘的事务，其不存在于以上两种变量的任意一个，因为其已经刷盘了，不需要我们再进行检查了，检查的目的其实就是为了潜在的崩溃恢复需求

因此，这2个`map`是描述我们数据库事务状态的核心数据，其必须进行持久化。在`Golang`中，持久化2个`map`是非常简单的，直接可以用`json`进行持久化（如果你之前做过cpp版本的持久化，那么你一定会赞美`Golang`）。

> 思考，一个事务刚提交时，肯定是没有刷盘的，那么时候我们知道其刷盘了呢？这时我们又需要做什么操作呢？


## 2.1 功能1-分配事务 id
首先事务管理器的基础职责之一就是分配事务`id`, 我们看看其中一个`put`接口:
> 这个接口实现复用了`Engine`中的原子变量, 这里直接给出实现进行讲解, 不需要你实现, 目的是讲解模块之间的耦合设计
```go
// GetNextTxnID returns the next transaction ID that will be assigned
func (m *TransactionManager) GetNextTxnID() uint64 {
	return atomic.LoadUint64(&m.engine.metadata.NextTxnID)
}
```

这里顺带补充我们的查询接口的设计, 你可以看到, 在没有开启事务的情况下, 即时是一次简单的`put`操作都会分配一个事务`id`, 因此这里的`tranc_id`是必须的, 其并不独属于我们的事务模块, 只是简单的`put/get/remove`操作数量只有一个而已(或者是一次性的`batch`接口, 总之不会有多步骤的操作)

因此，这里我们对于正儿八经的事务的操作，是需要分配`txn_id`的，对于简单的单次`put`, `get`等操作, 也需要分配`txn_id`, 因此对于这里的实现, `TransactionManager`复用`Engine`中的原子变量.

可以看到, 这里的`TransactionManager`和`Engine`是耦合的, 其操作依赖于`Engine`, 其实你将这些函数全部放到`Engine`中也是可以的, 只是这样会导致`Engin`太过于庞大, 因此这里单独抽离出了一个事务管理器。

## 2.2 功能2-分配事务上下文
回顾我们之前的Demo:
```go
auto tranc_hanlder = lsm.Begin()();
```
这里的`Begin()`会返回一个事务上下文(或者叫事务句柄也行), 我们可以在这个上下文中进行增删改查操作, 然后通过`commit`或`abort`函数完成提交事务或终结事务的流程. 我们看看这个上下文的定义:
```go
// Transaction represents a database transaction
type Transaction struct {
	id         uint64
	state      TransactionState
	isolation  IsolationLevel
	startTime  time.Time
	commitTime time.Time
	readTxnID  uint64 // Transaction ID used for reads (for snapshot isolation)
	manager    *TransactionManager
	mu         sync.RWMutex

	// Transaction-specific data storage for non READ_UNCOMMITTED levels
	tempMap     map[string]string          // Temporary storage for uncommitted writes
	readMap     map[string]*ReadRecord     // Read history for REPEATABLE_READ and SERIALIZABLE
	rollbackMap map[string]*RollbackRecord // Rollback information for READ_UNCOMMITTED
	operations  []*wal.Record              // WAL operations for this transaction
}

type TransactionState int

const (
	// TxnActive transaction is active
	TxnActive TransactionState = iota
	// TxnCommitted transaction is committed
	TxnCommitted
	// TxnAborted transaction is aborted/rolled back
	TxnAborted
)
```

可以看到, 事务上下文主要包含以下内容:

1. `id`: 事务`id`
2. `state`: 事务状态, 这里只有`TxnActive`, `TxnCommitted`和`TxnAborted`三个状态
3. `isolation`: 事务隔离级别
4. `startTime`/`commitTime`: 事务开始和结束时间, 这里记录的时间戳信息有助于实现更复杂的功能(后续Lab计划的`Bonus Lab`部分), 目前来说这两个变量暂时不重要
5. `readTxnID`: 读事务的`id`, 该字段目前没有使用, 你可以暂时忽略
6. `operations`: 事务操作记录, 也就是后续转化为`WAL`日志的内容
7. `tempMap`: 事务上下文中的临时数据, 主要是实现事务的隔离性, 例如`Read Committed`和`Repeatable Read`隔离级别下, 我们需要将`get`的数据暂存到这个临时数据中, 避免被其他事务读取到未提交的数据
8. `rollbackMap`: 事务回滚记录, 主要用于事务的回滚
9. `manager`: 事务管理器的指针

# 3 代码实现
这里我们进入今天的主题, 如何实现不同隔离级别下的事务操作。

本小节实验中，你需要修改的代码为：
- `pkg/lsm/txn_manager.go` 
- `pkg/lsm/txn_engine.go`

## 3.1 事务管理器部分
### 3.1.1 事务管理器初始化
```go
// pkg/lsm/txn_manager.go
// NewTransactionManager creates a new transaction manager
func NewTransactionManager(engine *Engine, config *TransactionConfig) *TransactionManager {
	// TODO: Lab 5.2

	return nil
}
```
这个函数构造一个新的事务管理器, 构造函数中需要传入`Engine`和`TransactionConfig`, 传入的`Engine`指针中就是赋值给`TransactionManager`中的`engine`成员变量的, 而`TransactionConfig`中保存了事务的配置信息, 例如事务的隔离级别等。

这个函数看上去简单, 但你别忘了, 它有可能是数据库重启时调用的, 因此你需要考虑事务的持久化信息的恢复工作. 因此, 你大概率需要再这个`NewTransactionManager`函数中调用`loadTxnStatus`来恢复事务状态信息。这个函数是我们的下一个任务.

### 3.1.2 事务信息的持久化
```go
// pkg/lsm/txn_manager.go
func (m *TransactionManager) syncTxnStatus() error {
	// TODO: Lab 5.2
	return nil
}

func (m *TransactionManager) loadTxnStatus() error {
	// TODO: Lab 5.2
	return nil
}
```
`syncTxnStatus`和`loadTxnStatus`函数用于持久化和恢复事务状态信息, `syncTxnStatus`函数用于将事务状态信息写入磁盘, `loadTxnStatus`函数用于从磁盘中恢复事务状态信息。

这里不对你的你的实现方式做限定, 但通常来说, 直接调用`go`的`json`包中的`Marshal`和`Unmarshal`函数来完成持久化和恢复工作是最简单的

### 3.1.3 分配事务上下文
这里我们从事务上下文的生命周期的历程逐步实现其关键的接口, 首先是分配函数:
```go
// pkg/lsm/txn_manager.go
// Begin starts a new transaction with default isolation level
func (m *TransactionManager) Begin() (*Transaction, error) {
	return m.BeginWithIsolation(m.config.DefaultIsolationLevel)
}

// BeginWithIsolation starts a new transaction with specified isolation level
func (m *TransactionManager) BeginWithIsolation(isolation IsolationLevel) (*Transaction, error) {
  // TODO: Lab 5.2
	return nil, nil
}
```
`BeginWithIsolation`函数用于分配并返回一个新的事务上下文。这个事物上下文的隔离级别由参数传入, 你需要完成的工作包括:
1. 初始化事务上下文结构特字段
2. 在事务管理器中记录事务信息


介于之前已经进行了详细的理论介绍, 这里就不过多介绍你需要进行哪些元数据的记录操作了。此外，类定义中的成员变量你不一定需要全部使用，你可以按照自己的理解选择性地使用预定义的成员变量，也可以自行添加新的成员变量。

## 3.2 事务CRUD接口
接下来是本小节内容的最重要部分，事务上下文接口的实现。这里不同隔离级别的事务操作实现会有所不同，你需要根据不同的事务隔离级别完成不同的`CRUD`逻辑:

> 以下的接口在实现`WAL`后, 你需要在实现接口时考虑`WAL`的持久化操作, 本实验中你暂时不需要考虑`WAL`的持久化操作。

### 3.2.1 put
```go
// pkg/lsm/txn_engine.go
// putWithTxn implements transaction-aware put operation based on isolation level
func (te *Engine) PutWithTxn(txn *Transaction, key, value string) error {
	// TODO: Lab 5.2
	return nil
}
```

这里首先介绍一下我们的接口的使用方式, 这里仍然是通过`Engine`结构体进行`CRUD`操作, 只是这种`xxxWithTxn`会携带上我们之前分配的事务上下文, 我们在`CRUD`时会根据这个事物上下文的信息进行操作.

其次, 有几个点需要你考虑:
1. 事务的可见性设计:
   1. `put`操作如何实现对其他事务的可见性?
   2. `put`操作如何实现对其他事务的隔离性?
2. 回滚设计
   1. 如果事务最后需要回滚, 如何实现?
   2. 回滚是否需要额外的数据结构?
> 这里你大概率需要使用到我们之前提到的`operations`, `tempMap`和`rollbackMap`


### 3.2.2 get
```go
// pkg/lsm/txn_engine.go
// getWithTxn implements transaction-aware get operation based on isolation level
func (te *Engine) GetWithTxn(txn *Transaction, key string) (string, bool, error) {
	// TODO: Lab 5.2
	return "", false, nil
}
```

这里需要考虑:

1. 如果是`Read UnCommitted`隔离级别, 需要考虑如何读取到最新的修改记录
2. 如果是`Read Committed`隔离级别, 需要考虑如何避免读取到未提交的数据
3. 如果是`Repeatable Read`隔离级别, 需要考虑如何避免不可重复读现象


### 3.2.3 delete
```go
// pkg/lsm/txn_engine.go
// deleteWithTxn implements transaction-aware delete operation based on isolation level
func (te *Engine) deleteWithTxn(txn *Transaction, key string) error {
	// TODO: Lab 5.2
	return nil
}
```
> 由于`remove`本质上也是`put`, 因此这里的逻辑和`put`类似, 这里就不做过多解释了。


### 3.2.4 批量化CRUD接口
```go
// pkg/lsm/txn_engine.go
// PutBatch inserts multiple key-value pairs using transaction context
func (te *Engine) PutBatchWithTxn(txn *Transaction, kvs []common.KVPair) error {
	// TODO: Lab 5.2

	return nil
}

// GetBatch retrieves multiple values by keys using transaction context
func (te *Engine) GetBatchWithTxn(txn *Transaction, keys []string) (map[string]string, error) {
	// TODO: Lab 5.2

	return nil, nil
}

// DeleteBatch marks multiple keys as deleted using transaction context
func (te *Engine) DeleteBatch(txn *Transaction, keys []string) error {
	// TODO: Lab 5.2

	return nil
}
```

这个接口和之前的逻辑是一样的，只是一次性进行了批量操作
> 这几个函数作者的参考分支并没有正确实现, 目的是希望你在没有参考代码的情况下独立完成

### 3.2.5 commit
```go
// pkg/lsm/txn_manager.go
// Commit commits the transaction
func (t *Transaction) Commit() error {
  // TODO: Lab 5.2
	return nil
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

### 3.2.5 Rollback
`Rollback` 方法用于回滚事务，具体的回滚逻辑取决于你之前对`put`函数的设计:
```go
// Rollback rolls back the transaction
func (t *Transaction) Rollback() error {
	// TODO: Lab 5.2
	return nil
}
```
这里同样用`nil`表示成功回滚, 否则返回`error`表示回滚失败。

> 在`commit`函数的冲突检测失败后, 也需要进行回滚操作, 不过其回滚是被动的
>
> 而`Rollback`函数是`client`主动发起的回滚操作


# 4 测试
现在除了崩溃恢复的部分外, 你应该可以通过的`pkg/lsm/txn_test.go`的测试