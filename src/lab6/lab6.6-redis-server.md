# Lab 6.6 redis-server实现
# 1 概述
之前我们完成了利用`KV`接口实现常见`Redis`命令的常见命令, 我们的`Lab`即将迎来最后一个部分, 即实现`Redis`服务器的部分。

这一部分内容其实也很简单，网络框架作者已经为你搭建好了，你只需要实现`Resp`请求的解析, 利用解析出的参数调用我们之前实现的各个命令的接口即可。

> 在初版的`Lab`中, 我们使用了`muduo`作为网络框架, 但由于`muduo`仅支持`Linux`, 因此在后续版本中我们将其替换为`asio`, 这是一个跨平台的网络库, 支持`Windows`, `MacOS`和`Linux`。使用`asio`后，本实验可以在所有主流操作系统上运行: `Linux`+`MacOS`+`Windows`(`Wsl`)

# 2 代码实现
这节课你可以修改`server`文件夹下的任何文件
```bash
├── server # 调用 Redis 兼容层的 Webserver
│   ├── include
│   │   └── handler.h # Redis 命令处理函数的声明
│   └── src
│       ├── handler.cpp # Redis 命令处理函数的实现, 就是对 redis_wrapper 的转发
│       └── server.cpp # Webserver 的实现
```

你需要实现的接口为:
```cpp
  std::string handleRequest(const std::string &request) {
    // TODO: Lab 6.6 处理网络传输的RESP字节流
    // TODO: Lab 6.6 形成参数并调用 redis_wrapper 的api
    // TODO: Lab 6.6 返回结果
    return "";
  }
```

> 如果你做得是初版的`Lab`, 这里实现的接口有所差异:
> ```cpp
> // server/src/server.cpp
> std::string handleRequest(const std::string &request) {
>   // TODO: Lab 6.6 处理网络传输的RESP字节流
>   // TODO: Lab 6.6 形成参数并调用 redis_wrapper 的api
>   // TODO: Lab 6.6 返回结果
> return "";
> }
> ```

`handleRequest`前后的网络包收发逻辑已经为你写好, 你只需要在这个函数中解析`RESP`协议, 调用`redis_wrapper`的接口即可。当然, 你也可以直接新增各种辅助函数。

> 除了我们之前实现的各种命令外, 你还需要实现`PING`命令, 这个命令不需要任何参数, 只需要返回`"+PONG\r\n"`即可。其内在含义表示服务器正在运行。

# 3 测试
你可以安装并使用`redis-cli`来测试你的`Redis`服务器, 过程为
```bash
cd  tiny-lsm
xmake
xmake run server
```
上述操作将启动你实现的`Redis`服务器

然后在另一个终端中运行:
```bash
redis-cli 
```
此时你就可以在`redis-cli`中输入我们之前`Lab`实现的命令和`server`进行交互了

预期的结果应该如下所示:

![redis-example](../images/lab6/redis-example.png)

# 4 redis-benchmark 压测
由于我们已经实现了支持`Resp`的服务器, 其提供类似官方的`redis-server`的服务。同时， `redis`官方提供了压测工具`redis-benchmark`, 我们可以利用其进行压测。

## 4.1 redis-benchmark 安装
`redis-benchmark`是一个官方的压测工具，无论你是从`apt/yum`等源安装的`redis`，还是从源码编译的，其默认安装选项中都提供了`redis-benchmark`。

## 4.2 redis-benchmark 使用
`redis-benchmark`的使用非常简单，只需要指定`-h`参数，指定redis服务器的ip地址，然后就可以进行压测了。

`redis-benchmark` 是 Redis 官方自带的性能测试工具，用于模拟多个客户端同时向 Redis 服务器发送命令，并评估其性能表现。它可以帮助你了解 Redis 在不同负载下的吞吐量和延迟情况。

基本的命令格式如下：

```bash
redis-benchmark [选项] [测试]
```

如果不带任何选项，`redis-benchmark` 会使用默认设置连接到本地 (127.0.0.1) 的 6379 端口，并执行一组预定义的测试。

---
### 常用选项

以下是一些常用的 `redis-benchmark` 选项：

* **主机和端口:**
    * `-h <hostname>`: 指定要连接的 Redis 服务器的主机名或 IP 地址 (默认为 `127.0.0.1`)。
    * `-p <port>`: 指定 Redis 服务器的端口号 (默认为 `6379`)。
    * `-a <password>`: 如果 Redis 服务器设置了密码，使用此选项进行身份验证。
    * `--tls`: 如果 Redis 服务器启用了 TLS/SSL 加密，使用此选项。

* **并发和请求数:**
    * `-c <clients>`: 指定并发客户端的数量 (默认为 `50`)。这模拟了同时有多少个连接在向 Redis 发送请求。
    * `-n <requests>`: 指定总的请求数量 (默认为 `100000`)。

* **数据大小:**
    * `-d <size>`: 指定 SET/GET 命令操作的数据大小，单位是字节 (默认为 `3`)。

* **测试命令:**
    * `-t <tests>`: 指定要执行的测试命令列表，用逗号分隔。例如 `-t SET,GET,LPUSH`。如果不指定，则执行默认的测试集 (PING_INLINE, PING_BULK, SET, GET, INCR, LPUSH, RPUSH, LPOP, RPOP, SADD, HSET, SPOP, LRANGE_100, LRANGE_300, LRANGE_500, LRANGE_600, MSET)。

* **Pipeline:**
    * `-P <numreq>`: 指定每个连接 pipeline 的请求数量 (默认为 `1`)。Pipeline 可以显著提高吞吐量，因为它允许客户端在等待前一个命令的响应之前发送多个命令。

* **随机性:**
    * `-r <keyspacelen>`: 对 SET/GET/INCR 等命令使用随机的 key，对 SADD 等命令使用随机的 value。`<keyspacelen>` 定义了随机 key 的范围后缀。例如，`-r 100000` 会生成类似 `mykey_rand:000000000000` 到 `mykey_rand:000000099999` 这样的 key。这有助于模拟更真实的缓存场景，避免总是命中同一个 key。

* **安静模式:**
    * `-q`: 安静模式，只输出 QPS (每秒查询数) 结果。

* **线程 (较新版本):**
    * `--threads <num>`: 指定使用的线程数 (默认为 `1`)。对于多核 CPU，使用多线程可以更好地压测 Redis。

* **集群模式:**
    * `--cluster`: 如果测试的是 Redis 集群，需要添加此参数。

---
### 输出结果解析

`redis-benchmark` 会为每个测试的命令输出类似以下的信息：

```
====== SET ======
  100000 requests completed in 1.23 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1

99.76% <= 1 milliseconds
100.00% <= 1 milliseconds
81300.81 requests per second
```

关键指标包括：

* **requests completed in ... seconds**: 完成指定请求数所花费的时间。
* **parallel clients**: 并发客户端数量。
* **bytes payload**: 数据负载大小。
* **...% <= ... milliseconds**: 不同百分位的请求延迟。例如，`99.76% <= 1 milliseconds` 表示 99.76% 的请求在 1 毫秒内完成。这有助于了解延迟的分布情况。
* **requests per second (QPS)**: 每秒处理的请求数。这是衡量 Redis 吞吐量的主要指标。

### 示例

* **测试本地 Redis 默认端口，执行默认测试集：**
    ```bash
    redis-benchmark
    ```

* **测试特定主机和端口，使用 100 个并发连接，总共 100 万个请求：**
    ```bash
    redis-benchmark -h myredishost -p 6380 -c 100 -n 1000000
    ```

* **只测试 SET 和 GET 命令，数据大小为 256 字节，使用 pipeline (每个 pipeline 16 个请求)：**
    ```bash
    redis-benchmark -t SET,GET -d 256 -P 16
    ```

* **安静模式，只看 QPS：**
    ```bash
    redis-benchmark -q
    ```

## 4.3 测试结果
我们指定如下测试选项:
```bash
# 1. 在一个 Terminal 中启动 server
➜  ~ xmake run server
# 2. 在另一个 Terminal 开启 redis-benchmark
➜  ~ redis-benchmark -h 127.0.0.1 -p 6379 -c 100 -n 100000 -q -t SET,GET,INCR,SADD,HSET,ZADD
WARNING: Could not fetch server CONFIG
SET: 142653.36 requests per second, p50=0.527 msec
GET: 134589.50 requests per second, p50=0.503 msec
INCR: 132802.12 requests per second, p50=0.503 msec
SADD: 131233.59 requests per second, p50=0.519 msec
HSET: 123456.79 requests per second, p50=0.583 msec
ZADD: 126422.25 requests per second, p50=0.615 msec
```

> 启动测试时, 你需要确保以下几点:
> 1. 使用`release`模型编译
> 2. 更改日志级别为`info`, 否则会输出大量日志, 影响测试结果。你如果确定代码没有bug，甚至可以关闭日志输出
> 3. 日志输出使用`reset_log_level`进行

这是作者的实现的测试结果, 同时我们可以用官方的`redis-server`测试进行对比:
```bash
# 1. 在一个 Terminal 中启动 server
➜  ~ redis-server
# 2. 在另一个 Terminal 开启 redis-benchmark
➜  ~  redis-benchmark -h 127.0.0.1 -p 6379 -c 100 -n 100000 -q -t SET,GET,INCR,SADD,HSET,ZADD
SET: 117647.05 requests per second, p50=0.527 msec
GET: 134228.19 requests per second, p50=0.527 msec
INCR: 132450.33 requests per second, p50=0.535 msec
SADD: 125628.14 requests per second, p50=0.551 msec
HSET: 128205.13 requests per second, p50=0.527 msec
ZADD: 137362.64 requests per second, p50=0.535 msec
```
这里我们对`SET,GET,INCR,SADD,HSET,ZADD`命令的实现的性能与`redis-server`的性能相近, 且有些许性能优势。你可以对比你自己的实现和`redis-server`的QPS, 预期的结果是与`redis-server`的QPS在同一数量级。

**压测之后呢?**
这里的压测本身不是我们的目的, 你需要关注的是压测反映出的性能不足问题, 并修改相应代码的实现。