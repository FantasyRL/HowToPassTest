### 6.5840 实验2：键值服务器

#### 介绍
在本实验中，你将构建一个单机键值服务器，确保每个操作即使在网络故障的情况下也能精确执行一次，并且操作是线性化的。客户端可以向键值服务器发送三个不同的RPC：`Put(key, value)`、`Append(key, arg)`和`Get(key)`。服务器维护一个键值对的内存映射。每个客户端通过`Clerk`与服务器通信，`Clerk`管理与服务器的RPC交互。

#### 开始
我们提供了`src/kvsrv`中的框架代码和测试。你需要修改`kvsrv/client.go`、`kvsrv/server.go`和`kvsrv/common.go`。

执行以下命令启动：
```sh
$ cd ~/6.5840
$ git pull
$ cd src/kvsrv
$ go test
```

#### 无网络故障的键值服务器
你的第一个任务是实现一个在无消息丢失情况下工作的解决方案。需要在`client.go`中添加RPC发送代码，并在`server.go`中实现`Put`、`Append`和`Get` RPC处理程序。当你通过测试套件的前两个测试：“一个客户端”和“多个客户端”时，任务完成。

#### 处理消息丢失的键值服务器
现在，你需要修改解决方案以应对消息丢失（例如RPC请求和回复）。`Clerk`可能需要多次发送RPC直到成功，每次调用`Clerk.Put()`或`Clerk.Append()`都应该只执行一次。

添加代码以在未收到回复时重试，并在`server.go`中过滤重复操作。这些笔记包括重复检测的指导。确保你的代码通过所有测试。

测试结果示例：
```sh
$ go test
Test: one client ...
  ... Passed -- t  3.8 nrpc 31135 ops 31135
Test: many clients ...
  ... Passed -- t  4.7 nrpc 102853 ops 102853
```

完整测试和代码实现详情请参阅[原文档](https://pdos.csail.mit.edu/6.824/labs/lab-kvsrv.html)。