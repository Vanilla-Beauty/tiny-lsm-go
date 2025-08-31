# Lab 4.5 Compact
# 1 概述
完成了`ConcactIterator`和`TwoMergeIterator`, 我们就已经可以开始实现我们的`Compact`流程了。

在LSM（Log-Structured Merge）树中，数据最初写入一个称为 **memtable** 的内存结构。一旦 memtable 达到一定大小限制，其内容会被刷写到磁盘上作为 SSTable（Sorted String Table）。随着时间的推移，可能会在 LSM 树的不同层级积累多个 SSTable。这些 SSTable 可能会导致以下问题：

- **读放大**：查询一个键时，系统可能需要搜索多个不同层级的 SSTable。这会增加 I/O 操作次数，从而降低读取性能。
- **写放大**：频繁的写操作生成新的 SSTable，最终需要与现有 SSTable 合并。如果没有压缩，这会导致过多的磁盘写入。
- **空间放大**：被删除或覆盖的键可能仍然存在于旧的 SSTable 中，直到它们通过压缩被清除。这浪费了磁盘空间。

为了解决这些问题，需要进行 **压缩（compaction）**。压缩将较小的 SSTable 合并成较大的 SSTable，移除过时的数据（如已删除或覆盖的键），并平衡各层级之间的数据分布。

由于`Compact`的策略对`LSM Tree`的性能有着绝对影响, 因此实际工业界的`Compact`策略非常多, 我们这一章先介绍不同的`Compact`策略, 再来实现一种最简单的`Compact`策略

# 2. 经典的SST压缩方案

这里先介绍一些经典的SST压缩方案。

## 2.1 基于大小的分层压缩（Size-Tiered Compaction）

这种方案按大小和层级对 SSTable 进行分组。每一层包含大致相同大小的 SSTable。当某一层的 SSTable 数量超过阈值时，它们会被合并成一个或多个 SSTable 并移动到下一层。

- **流程**：
  1. 将 Level L 的 SSTable 合并成一个或多个 SSTable 并移动到 Level L+1。
  2. 如果必要，递归执行此过程。

- **优点**：
  - 实现简单。
  - 高效处理高写入吞吐量。

- **缺点**：
  - 可能导致显著的读放大，因为单次查询可能需要扫描多个层级。

- **示意图**：
  ```text
  Level 0: [SST1] [SST2] [SST3]
  Level 1: [SST4] [SST5]
  Level 2: [SST6]

  压缩后：
  Level 0: []
  Level 1: [Merged(SST1,SST2,SST3)]
  Level 2: [SST6]
  ```

## 2.2 分层压缩（Leveled Compaction）

这种方案将 SSTable 分布在不同的层级，确保每个层级包含固定数量的数据。较低层级的数据以小块形式逐步移动到较高层级。

- **流程**：
  1. 将 Level L 的 SSTable 按键范围拆分成更小的部分，并与 Level L+1 中重叠的 SSTable 合并。
  2. 每次压缩只影响 Level L+1 中的一小部分 SSTable。

- **优点**：
  - 相比基于大小的分层压缩，减少了读放大。
  - 确保每个层级的键范围不重叠。

- **缺点**：
  - 实现复杂度更高。
  - 频繁的小规模合并导致较高的写放大。

- **示意图**：
  ```text
  Level 0: [SST1] [SST2] [SST3]
  Level 1: [RangeA] [RangeB] [RangeC]
  Level 2: [RangeD] [RangeE]

  压缩后:
  Level 0: []
  Level 1: [Merged(SST1,RangeA)] [Merged(SST2,RangeB)] [Merged(SST3,RangeC)]
  Level 2: [RangeD] [RangeE]
  ```

## 2.3 混合压缩（Hybrid Compacti2.3）

一些系统结合了基于大小的分层压缩和分层压缩的优点。例如，前几层使用基于大小的分层压缩，而较深层级切换到分层压缩。

- **优点**：
  - 在读放大和写放大之间取得平衡。
  - 根据工作负载特性提供灵活性。

- **缺点**：
  - 管理多种压缩策略增加了复杂性。


# 3 本项目 SST Compact 设计
本项目的`compact`机制采用了一种混合方法，主要在第0层使用基于大小的分层压缩，而在更高层则转向分层压缩。以下是详细的设计说明

## 3.1 触发压缩的时机

- **Level 0**：如果 Level 0 的 SSTable 数量超过一个阈值 `LSM_SST_LEVEL_RATIO`，则触发全量压缩，将所有 Level 0 的 SSTable 移动到 Level 1。
- **更高层**：如果某一层的 SSTable 数量超过 `LSM_SST_LEVEL_RATIO`，系统会递归检查下一层是否也需要压缩，然后继续执行。

> `LSM_SST_LEVEL_RATIO`定义在`config.toml`中

这里重点解释一下 `LSM_SST_LEVEL_RATIO`的含义, 它的含义是相邻两层的单个`SST`容量之比, 同时也是单层`SST`数量的阈值。这里我为了方便压缩触发的条件判断的便捷性, 规定每一个`Level`的`SST`数量阈值恒定。例如， 每层`SST`数量超过16时就需要进行`compact`, 这一层16个`SST`被合并为下一层的单个`SST`, 因此这个单个`SST`的容量是上一个`Level`中单个`SST`的容量的16倍。这个案例中, `LSM_SST_LEVEL_RATIO`就是16。

## 3.2 递归压缩

1. **确定源层和目标层**：
   - 确定源层 (`src_level`) 和目标层 (`src_level + 1`)。
   - 获取这两层的 SSTable ID。

2. **判断是否进行递归压缩**
   - 由于`src_level`层是触发了这一次`compact`操作, 因此其`SST`数量肯定大于等于`LSM_SST_LEVEL_RATIO`
   - 但还需要判断目标层(`src_level + 1`)的`SST`数量是否大于等于`LSM_SST_LEVEL_RATIO`。如果时, 则需要先将`src_level + 1`层的`SST`全部压缩到`src_level + 2`层
   - 上述的判断递归地在更高的`Level`中进行
  
3. **合并SSTable**：
   - 对于 `Level 0`，由于其不同`SST`中的`key`有重叠, 需要使用 `full_l0_l1_compact` 处理键范围重叠的问题
   - 对于更高层， 其`key`本来就是从小大大排布的, 使用 `full_common_compact` 合并不重叠键范围的 `SSTable`。

4. **生成新SSTable**：
   - 使用 `gen_sst_from_iter` 从合并后的迭代器结果生成新的 SSTable。
   - 确保新 SSTable 的大小符合 `get_sst_size` 定义的限制。

5. **更新元数据**：
   - 删除旧的 SSTable（从内存和磁盘中移除）。
   - 将新的 SSTable 添加到适当的目标层。
   - 更新 `level_sst_ids` 并对其进行排序以便高效访问。

> 这里的`full_l0_l1_compact`和`full_common_compact` 会在后文中介绍

**关键特性**

- **事务处理**：在压缩过程中，确保只将可见记录（基于事务ID）包含在新的 SSTable 中。
- **并发控制**：使用锁（`std::shared_mutex`）防止在访问共享资源（如 `ssts_mtx`）时发生竞争条件。
- **高效迭代**：利用迭代器（`TwoMergeIterator`、`HeapIterator`、`ConcactIterator`）高效遍历和合并来自多个 SSTable 的数据。

**示例流程**

假设初始状态如下：

- **压缩前**：
  ```text
  Level 0: [SST1] [SST2] [SST3]
  Level 1: [SST4] [SST5]
  Level 2: [SST6]
  ```

- **触发条件**：Level 0 有三个 SSTable，超过了阈值（`LSM_SST_LEVEL_RATIO = 2`）。

- **压缩过程**：
  1. 合并 `[SST1]`、`[SST2]` 和 `[SST3]` 成为一个迭代器。
  2. 将该迭代器的结果与 `[SST4]` 和 `[SST5]` 从 Level 1 中合并。
  3. 生成新的 SSTable 并将其分配到 Level 1。

- **压缩后**：
  ```text
  Level 0: []
  Level 1: [NewSST1] [NewSST2]
  Level 2: [SST6]
  ```

> TODO: 后续版本应该在此处添加一张流程图

# 4 本实验 Compact 策略的优势和劣势
本实验的`Compact`策略属于 `Leveled Compaction` (分层合并) 的一种变体。这种`全层合并`的`Leveled Compaction`策略，与其他主流策略的对比如下

> **Size-Tiered Compaction (STCS - 基于大小分层的合并)**
> 
> **Classic Leveled Compaction (经典的、更细粒度的分层合并，如LevelDB/RocksDB采用的策略)**

| 特性 (Aspect)             | 本实验策略 (Leveled - 全层合并)                                                                                                | Size-Tiered Compaction (STCS)                                                                                              | 经典Leveled Compaction (细粒度合并)                                                                                             |
| :------------------------ | :------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------- |
| **读放大 (Read Amplification)** | L0层: 中等 (需查找多个SSTable)<br>L1+层: **低** (由于SSTable不重叠且有序，每层理论上最多查找一个SSTable)                                 | **高** (通常需要在多个层级(Tier)的多个SSTable中查找，因为不同SSTable间的键范围可能大量重叠)                                              | L0层: 中等 (需查找多个SSTable)<br>L1+层: **低** (与本实验的策略类似，L1+层SSTable不重叠)                                                    |
| **写放大 (Write Amplification)** | **非常高** (合并Level\_N和Level\_{N+1}时，两个层级的*全部*数据都会被读取和重写，即使Level\_{N+1}中大部分是“冷”数据)                          | **低** (数据在其生命周期中被重写的次数相对较少，通常是合并固定数量的SSTable)                                                              | **中等** (比STCS高，但显著低于“全层合并”。仅合并Level\_N中少量被选中的SSTable及其在Level\_{N+1}中的键范围重叠部分)                             |
| **空间放大 (Space Amplification)** | **低** (合并过程频繁且较为彻底地重写数据，有利于快速回收无效数据和墓碑(Tombstone)占用的空间)                                              | **高** (旧版本数据和墓碑可能长时间存在于未被合并的SSTable中，导致总体磁盘占用较大，最坏情况一个键的多个版本都存在)                                | **低** (与“全层合并”类似，能较好控制空间。因单次合并范围较小，整体回收速度可能略慢于全层合并，但通常有严格的层级大小限制)                             |
| **合并I/O成本 (Compaction I/O Cost)** | **高** (单次合并涉及的数据量通常很大，导致瞬时I/O和CPU压力显著，可能引发性能抖动或“合并风暴”)                                              | **中低** (通常合并固定数量（例如N个）的SSTable，单次合并成本相对可控，但合并操作可能更频繁)                                                  | **中等** (单次合并的数据量介于STCS和“全层合并”之间，致力于平滑I/O负载)                                                              |
| **实现复杂度** | **中等** (比STCS复杂，因为需要维护层级结构和SSTable元数据；但比经典的细粒度Leveled Compaction简单，因为合并整个层级的逻辑较直接)             | **低** (逻辑相对简单，主要是基于SSTable的大小和数量触发合并，易于实现和维护)                                                              | **高** (需要复杂的SSTable挑选逻辑(picking logic)来决定哪些SSTable参与合并，键范围管理、并发控制以及避免写暂停等机制都较复杂)               |
| **墓碑/旧数据清理效率** | **高** (由于整个层级的数据会定期参与合并过程，墓碑和旧版本数据能得到较快和较彻底的清理)                                                            | **低** (清理效率依赖于包含墓碑或旧数据的SSTable何时被选中参与合并，可能存在较长延迟)                                                          | **中高** (数据也定期参与合并过程，但由于单次合并涉及的数据范围小于全层合并，整体清理速度和彻底性可能稍逊于全层合并，但仍属高效)                        |
| **查询性能稳定性/可预测性** | L1+层: **好** (查询路径短且固定，性能稳定)<br>L0层: 一般 (受SSTable数量影响)                                                                | **差** (查询性能波动较大，取决于数据具体分布在哪些SSTable以及需要检查的SSTable数量)                                                          | L1+层: **好** (查询路径短且固定，性能稳定)<br>L0层: 一般 (受SSTable数量影响)                                                          |

# 5 本实验的代码实现
在完成之前的理论学习后, 你可以开始实现本实验的代码了

本小节你需要更改的代码文件为:
- `src/lsm/engine.cpp`
- `include/lsm/engine.h` (Optional)

## 5.1 新`SST`的构造
`gen_sst_from_iter`从一个迭代器中构造新的`SST`, 新的`SST`的容量上限为`target_sst_size`, 新的`SST`的层级为`target_level`。 也就是说, 假设迭代器中所有键值对的容量是`128 MB`, 而`target_sst_size = 32MB`, 那么你需要构造4个`SST`。
```go
std::vector<std::shared_ptr<SST>>
LSMEngine::gen_sst_from_iter(BaseIterator &iter, size_t target_sst_size,
                             size_t target_level) {
  // TODO: Lab 4.5 实现从迭代器构造新的 SST

  return {};
}
```

这里, `SST`的命名规则参照`get_sst_path`:
```go
std::string LSMEngine::get_sst_path(size_t sst_id, size_t target_level) {
  // sst的文件路径格式为: data_dir/sst_<sst_id>，sst_id格式化为32位数字
  std::stringstream ss;
  ss << data_dir << "/sst_" << std::setfill('0') << std::setw(32) << sst_id
     << '.' << target_level;
  return ss.str();
}
```

> `sst`的`id`的分配可以简单地按照如下操作获取:
> ```go
> size_t sst_id = next_sst_id++
> ```

## 5.2 full_l0_l1_compact
`full_l0_l1_compact`负责将`L0`层和`L1`层的`SST`合并到`L1`层, 因为`L0`层的`SST`之间是不排序且存在重叠的, 因此你需要结合之前实现的迭代器对其进行排序和去重, 并和`L1`迭代器整合成新的迭代器, 出入你刚刚实现的`gen_sst_from_iter`函数, 完成新的`SST`的构造:
```go
std::vector<std::shared_ptr<SST>>
LSMEngine::full_l0_l1_compact(std::vector<size_t> &l0_ids,
                              std::vector<size_t> &l1_ids) {
  // TODO: Lab 4.5 负责完成 l0 和 l1 的 full compact
  return {};
}
```

> 根据我们的`Compact`策略设计, `Ln`层的`SST`容量应该是`Ln-1`层的`LSM_SST_LEVEL_RATIO`倍, 作者已经提供了`get_sst_size`帮助你计算任意一层`SST`的容量

## 5.2 full_common_compact
`full_common_compact`负责其他相邻`Level`的`SST`合并, 你需要参考`full_l0_l1_compact`的实现, 完成其他相邻`Level`的`SST`合并。这里应该会更简单，因为这里相邻的两个`Level`的`SST`之间是排序且不重叠的, 因此单个`Level`的迭代器都是相同的类型:
```go
std::vector<std::shared_ptr<SST>>
LSMEngine::full_common_compact(std::vector<size_t> &lx_ids,
                               std::vector<size_t> &ly_ids, size_t level_y) {
  // TODO: Lab 4.5 负责完成其他相邻 level 的 full compact

  return {};
}
```

# 5.3 full_compact
`full_compact`负责整个`Compact`流程, 你需要根据`Compact`策略设计, 完成这个函数。另外, 由于每次`compact`会导致目标`Level`的`SST`数量增加, 因此这个`compact`流程可能会哦递归地进行, 你需要在`full_compact`中控制这个递归过程。也就是说，你需要按照我们之前描述的策略控制之前实现的`full_common_compact`和`full_l0_l1_compact`的调用:
```go
void LSMEngine::full_compact(size_t src_level) {
  // TODO: Lab 4.5 负责完成整个 full compact
  // ? 你可能需要控制`Compact`流程需要递归地进行
}
```

<details>
<summary>点击这里展开/折叠提示 (建议你先尝试自己完成)</summary>

这里给出作者对这个函数的的实现思路:

`LSMEngine::full_compact` 函数的目标是将指定层级 `src_level` 的所有 SSTable 压缩到下一层级 `src_level + 1`，并通过合并和优化减少冗余数据，提升读写性能。其步骤为:

**a. 递归检查下一层是否需要压缩**
- 在对当前层级（`src_level`）进行全量压缩之前，先检查下一层级（`src_level + 1`）是否也需要进行全量压缩。
- 如果 `src_level + 1` 层的 SSTable 数量超过阈值（`LSM_SST_LEVEL_RATIO`），则递归调用 `full_compact(src_level + 1)` 对下一层进行压缩。
- 这种递归机制确保了在压缩当前层之前，目标层已经处于优化状态。

**b. 获取源层级和目标层级的 SSTable ID**
- 从 `level_sst_ids[src_level]` 和 `level_sst_ids[src_level + 1]` 中分别获取当前层级和目标层级的所有 SSTable ID。
- 将这些 ID 转换为两个向量：`lx_ids`（源层级）和 `ly_ids`（目标层级），便于后续处理。

**c. 根据层级选择不同的压缩方式**
- 根据 `src_level` 是否为 Level 0，选择不同的压缩方法：
  - **Level 0**：由于 Level 0 的 SSTable 可能存在键范围重叠，调用 `full_l0_l1_compact(lx_ids, ly_ids)` 处理。
  - **其他层级**：调用 `full_common_compact(lx_ids, ly_ids, src_level + 1)` 处理，这些层级的 SSTable 键范围不重叠。

**d. 移除旧的 SSTable**
- 压缩完成后，删除源层级和目标层级中所有旧的 SSTable：
  - 调用 `del_sst()` 方法释放磁盘资源。
  - 从 `ssts` 映射中移除这些 SSTable。
- 清空 `level_sst_ids[src_level]` 和 `level_sst_ids[src_level + 1]`，为新生成的 SSTable 准备空间。

**e. 更新最大层级**
- 更新 `cur_max_level`，确保其始终表示当前 LSM 树中的最大层级。

**f. 添加新的 SSTable**
- 将新生成的 SSTable 添加到目标层级（`src_level + 1`）：
  - 将新 SSTable 的 ID 插入到 `level_sst_ids[src_level + 1]`。
  - 将新 SSTable 本身插入到 `ssts` 映射中。
- 对 `level_sst_ids[src_level + 1]` 进行排序，确保 SSTable 按顺序存储，便于高效访问。
</details>

## 5.4 Compact 的触发时机
最后, `full_compact`的调用肯定需要有一个触发时机, 你可以选择在`put`时候惰性地检查每层`Level`的`SST`数量是否达到阈值, 也可以单独创建一个线程进行轮训检查, 具体实现方案取决于你自己, 因此这里就没有对指定的函数进行挖空让你实现了。

# 6 测试
这一章的测试有一点特别, 由于我们常规测试中为了速度考虑, 不会有大量的键值对插入行为。而测试`Compact`, 需要数据量较大时, 才会触发多个`SST`文件的持久化以及超出数量后的`compact`操作, 因此这里的建议是, 将配置文件中单个`L0 SST`的大小(也就是单个`Skiplist`的大小)调小(其他相应参数也一并调小), 然后运行`Persistence`函数。

例如我如下调整配置：
```toml
[lsm.core]
# Memory table size limit (64MB)
# LSM_TOL_MEM_SIZE_LIMIT = 67108864 # Calculated from 64 * 1024 * 1024 # 原来的 MemTable 容量限制
LSM_TOL_MEM_SIZE_LIMIT = 65536 # Calculated from 64 * 1024 * 1024
# Per-memory table size limit (4MB)
# LSM_PER_MEM_SIZE_LIMIT = 4194304 # Calculated from 4 * 1024 * 1024 # 原来的单个 Skiplist 容量限制
LSM_PER_MEM_SIZE_LIMIT = 8192 # Calculated from 4 * 1024 * 1024
# Block size (32KB)
# LSM_BLOCK_SIZE = 32768 # Calculated from 32 * 1024 # # 原来的单个 Block 容量限制
LSM_BLOCK_SIZE = 1024 # Calculated from 32 * 1024
# SST level size ratio
LSM_SST_LEVEL_RATIO = 4
```

> 需要注意的是, 单元测试读取配置文件默认是当前目录下的`config.toml`因此你需要将修改后的配置文件复制到编译单元测试的目录, 一般是:
> ```bash
> ./build/linux/x86_64/release # release 版本
> ./build/linux/x86_64/debug # debug 版本
> ```

你只需要关注这个测例:
```go
// test/test_lsm.cpp
TEST_F(LSMTest, Persistence) {
    // ...
}
```

通过这个测试即可

> TODO: 后期实验项目书应进行优化, 不应该让学员手动来控制这一过程
