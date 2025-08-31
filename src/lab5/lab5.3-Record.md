# Lab 5.3 WAL日志编解码
之前我们已经完成了数据库内存中和编码文件中事务信息的融入, 现在我们还需要加上`WAL`与崩溃恢复的内容。
这一小节我们主要对`WAL`中的单条日志记录 (即`Record`) 进行编解码设计和实现。

# 1 Record 编码设计
## 1.1 WAL简介
这里首先简单介绍下**WAL（预写式日志）** , **WAL（预写式日志）** 是数据库系统中保障数据一致性和事务可靠性的核心技术，核心思想是“**日志先行**”——任何数据修改必须先记录到日志中，确保日志持久化后，才允许实际数据写入磁盘。这种机制解决了两个关键问题：一是保证已提交的事务不会因系统崩溃而丢失（持久性），二是确保事务要么完全生效、要么完全回滚（原子性）。

具体来说，当一个事务提交时，数据库会先将事务的修改操作（例如数据修改前后的值、事务状态等）按顺序写入日志文件，并强制将日志刷到磁盘存储。由于日志是顺序写入，相比随机修改数据页的I/O操作，这种设计大幅提升了性能。之后，数据库可以灵活地将内存中的脏页批量刷新到磁盘，减少磁盘操作次数。

如果系统崩溃，重启后可通过日志恢复数据。恢复分为两个阶段：**Redo** 阶段会重放所有已提交但未落盘的日志，确保事务修改生效；**Undo** 阶段则回滚未提交事务的部分修改，消除中间状态。为了加速恢复，数据库会定期创建**检查点（Checkpoint）**，将当前内存中的脏页刷盘，并记录日志位置，这样恢复时只需处理检查点之后的日志。

WAL的优势不仅在于数据安全，还在于其高性能和可扩展性。例如，PostgreSQL、MySQL InnoDB等数据库依赖WAL实现事务和崩溃恢复；分布式系统（如Raft算法）也借鉴类似思想，通过日志复制保证一致性。不过，WAL的日志文件可能快速增长，需要定期清理或归档，且频繁刷盘可能带来性能损耗，因此在实际应用中需权衡同步/异步提交等策略。

## 1.2 文件格式设计
根据之前的介绍, 我们的`WAL`文件中需要满足如下功能:
1. 描述事务的开始和结束
2. 描述每次事务的操作内容

在`KV`存储引擎这个领域, `WAL`的设计已经比关系型数据库简单很多了, 因为其操作类型就只有简单的基于键值对的`put/get/remove`. 与之相反, 关系型数据库的`WAL`就复杂很多了, 还涉及到物理日志和逻辑日志的区别

在KV存储引擎中，`WAL`的设计需要满足简洁性和高效性。由于操作类型仅限于`put`、`remove`（`get`通常不涉及数据修改，因此无需记录），日志结构可以大幅简化。以下是具体的设计要点：

每个日志条目需包含以下核心信息：
1. **事务标识（Transaction ID）**：唯一标识事务的ID，用于关联多个操作。
2. **操作类型（Operation Type）**：如`PUT`、`REMOVE`、`GET`、`BEGIN`、`COMMIT`、`ABORT`等。
3. **键（Key）**：操作的键值。
4. **值（Value）**：对于`PUT`操作记录具体值；
5. **校验和（Checksum）(可选)**：用于验证日志条目的完整性（如CRC32）。
6. **时间戳（可选）**：记录操作时间，用于多版本控制或冲突解决。

这样，我们可以通过每一个日志条目判断这个操作类型和数据、是哪一个事务进行操作。同时， 由于我们在上一章中将数据库的事务完成状态也进行了持久化, 因此在崩溃恢复时, 我们可以通过检查当前`WAL`条目的事务`id`和以刷盘的事务的状态进行对比, 来判断是否需要重放操作。

## 1.3 Record 代码概览
基于我们之前的描述, 我们来看看`Record`中每一类记录项的定义:
```go
// include/wal/record.h
class Record {
private:
  // 构造函数
  Record() = default;

public:
  // 操作类型枚举

  static Record createRecord(uint64_t tranc_id);
  static Record commitRecord(uint64_t tranc_id);
  static Record rollbackRecord(uint64_t tranc_id);
  static Record putRecord(uint64_t tranc_id, const std::string &key,
                          const std::string &value);
  static Record deleteRecord(uint64_t tranc_id, const std::string &key);

  // 编码记录
  std::vector<uint8_t> encode() const;

  // 解码记录
  static std::vector<Record> decode(const std::vector<uint8_t> &data);

  // ...

private:
  uint64_t tranc_id_;
  OperationType operation_type_;
  std::string key_;
  std::string value_;
  uint16_t record_len_;
};
```
这里展示了几个关键的成员变量和成员函数, 这里的`Record`表示的就是`WAL`中的单个日志条目, 操作类型为`OperationType`, 通过`OperationType`可以判断其是否有`key`, `value`等附加数据信息。同时，我们通过静态成员函数`createRecord`, `putRecord`等构造类的实例。最后，`encode`和`decode`函数用于将记录转换为字节流和从字节流恢复记录。

> 这里的构造函数被标记为`private`, 你需要使用`createRecord`等静态成员函数来构造`Record`的实例。

## 1.4 Record 文件格式
`Record`仅仅是内存中的一个类, 且其会因记录类型的不同导致占据的内存大小不同, 因此我们需要将采用某种编码格式将其序列化到磁盘上, 以便在崩溃恢复时能够从磁盘上恢复出`Record`。这里我们采用一种简单的序列化方式, 将每一个`Record`的长度和内容依次写入磁盘, 具体格式如下:
```text
| record_len | tranc_id | operation_type | key_len(optional) | key(optional) | value_len(optional) | value(optional) |
```
这里, 当`operation_type`是`CREATE`, `ROLLBACK`, `COMMIT`时, 只需要记录`tranc_id`和`operation_type`即可, 其余的`optional`部分不存在, 当`operation_type`是`PUT`时, 需要记录`tranc_id`, `operation_type`, `key`, `value`; 当`operation_type`是`DELETE`时, 需要记录`tranc_id`, `operation_type`, `key`。'

每个条目的第一部分是`record_len`, 其记录了整个日志条目的长度(16位)。这里的编解码需要注意一下，`encode`函数是以单个`Record`为单位, 将其编码为字节流, 而`decode`函数是以字节流为单位, 将其解码为`Record`数组。

# 2 代码实现
现在你已经了解了`Record`的设计和编码格式, 接下来你需要实现`Record`的基础构造函数和编解码函数。

你需要修改的文件包括：
- `src/wal/record.cpp`
- `include/wal/record.h` (Optional)

## 2.1 构造函数
这里的构造函数其实是一系列静态函数, 其们会根据不同的操作类型构造不同的`Record`实例, 你需要实现这些静态函数:
```go
Record Record::createRecord(uint64_t tranc_id) {
  // TODO: Lab 5.3 实现创建事务的Record
  return {};
}
Record Record::commitRecord(uint64_t tranc_id) {
  // TODO: Lab 5.3 实现提交事务的Record
  return {};
}
Record Record::rollbackRecord(uint64_t tranc_id) {
  // TODO: Lab 5.3 实现回滚事务的Record
  return {};
}
Record Record::putRecord(uint64_t tranc_id, const std::string &key,
                         const std::string &value) {
  // TODO: Lab 5.3 实现插入键值对的Record
  return {};
}
Record Record::deleteRecord(uint64_t tranc_id, const std::string &key) {
  // TODO: Lab 5.3 实现删除键值对的Record
  return {};
}
```

> 之所以这样设计, 是因为不同类型的`Record`的成员变量数量不同, 比如`CREATE`类型的`Record`只需要`tranc_id`和`operation_type`两个成员变量, 而`PUT`类型的`Record`则需要`tranc_id`, `operation_type`, `key`, `value`四个成员变量, 因此我们通过静态函数来构造不同的`Record`实例, 这样可以避免构造函数的参数过多。

## 2.2 编解码函数
接下来是接触的编解码函数, 这里你只需要编解码成字节数组即可, 文件IO相关操作你在下一个`Lab`实现:
```go
std::vector<uint8_t> Record::encode() const {
  // TODO: Lab 5.3 实现Record的编码函数
  return {};
}

std::vector<Record> Record::decode(const std::vector<uint8_t> &data) {
  // TODO: Lab 5.3 实现Record的解码函数
  return {};
}
```

> TODO: 初版实验代码中, `encode`和`decode`是针对单个`Record`进行的, 后续版本应进行改进, 使编解码的数据以`std::vector<Record>`为单位, 这样可以避免内存的频繁分配和释放。

# 3 测试
本小节没有单元测试, 你在完成下一小节后会有统一的单元测试。
