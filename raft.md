## 介绍

这是系列实验中的第一个，在这些实验中你将构建一个容错的键/值存储系统。在本实验中，你将实现Raft，一个复制状态机协议。在下一个实验中，你将基于Raft构建一个键/值服务。然后你将把服务“分片”到多个复制状态机上以提高性能。

一个复制的服务通过在多个副本服务器上存储其状态（即数据）的完整副本来实现容错。复制允许服务在某些服务器出现故障（崩溃或网络故障）时继续运行。挑战在于故障可能导致副本持有不同的数据副本。

Raft将客户端请求组织成一个序列，称为日志，并确保所有副本服务器看到相同的日志。每个副本按照日志顺序执行客户端请求，将它们应用到本地副本的服务状态上。由于所有在线的副本看到相同的日志内容，它们按相同的顺序执行相同的请求，因此继续保持相同的服务状态。如果一个服务器故障但随后恢复，Raft会负责将其日志更新到最新状态。只要大多数服务器仍然存活并能够互相通信，Raft就会继续运行。如果没有这样的多数，Raft将无法取得进展，但一旦多数能够再次通信，它将从中断的地方继续运行。

在本实验中，你将实现Raft作为一个具有相关方法的Go对象类型，旨在作为更大服务的一个模块。一组Raft实例通过RPC相互通信以维护复制日志。你的Raft接口将支持一个无限序列的编号命令，也称为日志条目。这些条目用索引号编号。具有给定索引的日志条目最终将被提交。此时，你的Raft应将日志条目发送给更大的服务以执行。

你应遵循扩展Raft论文中的设计，特别注意图2。你将实现论文中的大部分内容，包括保存持久状态并在节点故障后重新启动时读取它。你将不实现集群成员变更（第6节）。

本实验分四部分提交。你必须在对应的截止日期提交每一部分。

## 入门

如果你已经完成了实验1，你已经有了一份实验源代码。如果没有，你可以在实验1说明中找到通过git获取源代码的指示。

我们提供了src/raft/raft.go中的骨架代码。我们还提供了一组测试，你应使用这些测试来推动你的实现工作，我们也将使用这些测试来评分你的提交实验。这些测试在src/raft/test_test.go中。

在我们评分你的提交时，我们将运行不带-race标志的测试。然而，你应在开发解决方案时通过带-race标志运行测试以检查代码是否存在竞争。

要启动并运行，执行以下命令。别忘了通过git pull获取最新的软件。

```bash
$ cd ~/6.5840
$ git pull
...
$ cd src/raft
$ go test
Test (3A): initial election ...
--- FAIL: TestInitialElection3A (5.04s)
        config.go:326: expected one leader, got none
Test (3A): election after network failure ...
--- FAIL: TestReElection3A (5.03s)
        config.go:326: expected one leader, got none
...
$
```

## 代码实现

通过向`raft/raft.go`添加代码来实现Raft。在该文件中，你会找到骨架代码以及发送和接收RPC的示例。
你的实现必须支持以下接口，测试器和（最终的）键/值服务器将使用该接口。你可以在`raft.go`的注释中找到更多细节。

```go
// 创建一个新的Raft服务器实例：
rf := Make(peers, me, persister, applyCh)

// 开始就一个新的日志条目达成一致：
rf.Start(command interface{}) (index, term, isleader)

// 询问Raft其当前任期，并判断它是否认为自己是领导者：
rf.GetState() (term, isLeader)

// 每次有一个新的条目提交到日志中，每个Raft节点都应该发送一个ApplyMsg到服务（或测试器）。
type ApplyMsg
```

一个服务调用`Make(peers, me, ...)`来创建一个Raft节点。`peers`参数是Raft节点的网络标识符数组（包括这个节点），用于RPC。`me`参数是此节点在`peers`数组中的索引。`Start(command)`请求Raft开始处理将命令附加到复制日志。`Start()`应立即返回，而不等待日志附加完成。服务期望你的实现为每个新提交的日志条目发送一个`ApplyMsg`到`Make()`的`applyCh`通道参数。

`raft.go`包含发送RPC（`sendRequestVote()`）和处理传入RPC（`RequestVote()`）的示例代码。你的Raft节点应使用`labrpc` Go包（源代码在`src/labrpc`）交换RPC。测试器可以告诉`labrpc`延迟RPC、重新排序它们以及丢弃它们以模拟各种网络故障。虽然你可以暂时修改`labrpc`，但确保你的Raft在原始`labrpc`下工作，因为我们将使用它来测试和评分你的实验。你的Raft实例必须仅与RPC交互；例如，它们不允许使用共享的Go变量或文件进行通信。

随后的实验将基于此实验，因此为自己留出足够的时间编写可靠的代码非常重要。

---

## Part 3A：领导者选举（中等难度）

### 任务
实现Raft的领导者选举和心跳（不包含日志条目的AppendEntries RPCs）。Part 3A的目标是选举一个领导者，如果没有故障，领导者将保持领导者地位，如果旧领导者失效或数据包丢失，则新的领导者接替。运行 `go test -run 3A` 来测试你的3A代码。

### 提示：
- 你不能直接运行你的Raft实现；相反，你应该通过测试器运行它，即 `go test -run 3A`。
- 遵循论文的图2。在此时，你需要关注发送和接收RequestVote RPCs、与选举相关的服务器规则以及与领导者选举相关的状态。
- 将图2中与领导者选举相关的状态添加到`raft.go`中的Raft结构体中。你还需要定义一个结构体来保存每个日志条目的信息。
- 填充`RequestVoteArgs`和`RequestVoteReply`结构体。修改`Make()`以创建一个后台协程，当在一段时间内没有收到其他节点的消息时，周期性地启动领导者选举，发送RequestVote RPCs。实现`RequestVote()` RPC处理程序，使服务器相互投票。
- 为了实现心跳，定义一个`AppendEntries` RPC结构体（尽管你可能不需要所有参数），并让领导者定期发送它们。编写一个`AppendEntries` RPC处理程序方法。
- 测试器要求领导者发送心跳RPC的频率不超过每秒十次。
- 测试器要求你的Raft在旧领导者失效五秒内选出新的领导者（如果大多数节点仍然可以通信）。
- 论文的第5.2节提到选举超时范围为150到300毫秒。这样的范围只有在领导者发送心跳的频率明显高于每150毫秒一次时才有意义（例如，每10毫秒一次）。由于测试器限制你每秒发送不超过十次心跳，你将不得不使用比论文的150到300毫秒更大的选举超时，但不能太大，因为那样可能无法在五秒内选出领导者。
- 你可能会发现Go的`rand`有用。
- 你需要编写代码来周期性地或在时间延迟后执行动作。最简单的方法是创建一个带有循环调用`time.Sleep()`的协程；参见`Make()`为此目的创建的`ticker()`协程。不要使用Go的`time.Timer`或`time.Ticker`，它们难以正确使用。
- 如果你的代码在通过测试时遇到困难，请再次阅读论文的图2；领导者选举的完整逻辑分布在图的多个部分。
- 别忘了实现`GetState()`。
- 当测试器永久关闭一个实例时，会调用你的Raft的`rf.Kill()`。你可以使用`rf.killed()`检查是否已调用`Kill()`。你可能想在所有循环中进行此检查，以避免关闭的Raft实例打印混乱的信息。
- Go RPC仅发送字段名以大写字母开头的结构体字段。子结构体也必须有大写的字段名（例如数组中日志记录的字段）。`labgob`包会警告你这些问题；不要忽略警告。
- 本实验最具挑战性的部分可能是调试。花些时间让你的实现易于调试。参考指导页面获取调试技巧。
- 在提交Part 3A之前，确保通过3A测试，以便你看到类似如下的内容：

```bash
$ go test -run 3A
Test (3A): initial election ...
  ... Passed --   3.5  3   58   16840    0
Test (3A): election after network failure ...
  ... Passed --   5.4  3  118   25269    0
Test (3A): multiple elections ...
  ... Passed --   7.3  7  624  138014    0
PASS
ok      6.5840/raft    16.265s
$
```

每行“Passed”包含五个数字；这些数字是测试所用的时间（秒）、Raft节点的数量、测试期间发送的RPC数量、RPC消息的总字节数和Raft报告提交的日志条目数。你的数字会与这里显示的不同。如果你愿意，可以忽略这些数字，但它们可能有助于你检查实现发送的RPC数量。对于所有的实验3、4和5，如果所有测试（`go test`）超过600秒或任何单个测试超过120秒，评分脚本将失败你的解决方案。

在我们评分你的提交时，我们将运行不带`-race`标志的测试。然而，你应该确保你的代码始终通过带`-race`标志的测试。

## Part 3B：日志（难度大）

### 任务
实现领导者和跟随者代码以追加新的日志条目，以便通过 `go test -run 3B` 测试。

### 提示
- 运行 `git pull` 获取最新的实验软件。
- 你的首要目标应该是通过 `TestBasicAgree3B()` 测试。先实现 `Start()`，然后编写代码，通过 `AppendEntries` RPC 发送和接收新日志条目，遵循图2。在每个节点上将每个新提交的条目发送到 `applyCh`。
- 你需要实现选举限制（论文第5.4.1节）。
- 你的代码可能包含反复检查某些事件的循环。不要让这些循环在没有暂停的情况下连续执行，因为这会减慢你的实现速度，导致测试失败。使用Go的条件变量，或在每次循环迭代中插入 `time.Sleep(10 * time.Millisecond)`。
- 为了将来更容易实现，请编写（或重写）代码，使其干净明了。获取想法，请重新访问指导页面，获取开发和调试代码的提示。
- 如果你测试失败，请查看 `test_test.go` 和 `config.go` 以了解测试的内容。`config.go` 还展示了测试器如何使用 Raft API。
- 即将到来的实验测试可能会因为代码运行太慢而导致失败。你可以使用 `time` 命令检查你的解决方案使用了多少实际时间和CPU时间。下面是典型输出：

```bash
$ time go test -run 3B
Test (3B): basic agreement ...
  ... Passed --   0.9  3   16    4572    3
Test (3B): RPC byte count ...
  ... Passed --   1.7  3   48  114536   11
Test (3B): agreement after follower reconnects ...
  ... Passed --   3.6  3   78   22131    7
Test (3B): no agreement if too many followers disconnect ...
  ... Passed --   3.8  5  172   40935    3
Test (3B): concurrent Start()s ...
  ... Passed --   1.1  3   24    7379    6
Test (3B): rejoin of partitioned leader ...
  ... Passed --   5.1  3  152   37021    4
Test (3B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  17.2  5 2080 1587388  102
Test (3B): RPC counts aren't too high ...
  ... Passed --   2.2  3   60   20119   12
PASS
ok      6.5840/raft    35.557s

real    0m35.899s
user    0m2.556s
sys     0m1.458s
$
```

"ok 6.5840/raft 35.557s" 表示Go测量了3B测试的时间为35.557秒的实际（挂钟）时间。
"user 0m2.556s" 表示代码消耗了2.556秒的CPU时间，即实际执行指令（而不是等待或睡眠）的时间。如
果你的解决方案在3B测试中使用的实际时间超过1分钟，或CPU时间超过5秒，你可能会在后面遇到麻烦。检查花费时
间的睡眠或等待RPC超时的部分，不断运行的循环没有睡眠或等待条件或通道消息，或发送了大量RPC。

## Part 3C：持久化（难度大）

### 任务
如果基于Raft的服务器重新启动，它应该从上次停止的地方恢复服务。这要求Raft保留能在重启后存活的持久化状态。论文的图2中提到了哪些状态应该持久化。

一个真实的实现会在每次状态变化时将Raft的持久化状态写入磁盘，并在重启后从磁盘读取状态。你的实现不会使用磁盘；相反，它将使用`Persister`对象保存和恢复持久化状态（见`persister.go`）。调用`Raft.Make()`的人会提供一个`Persister`，该`Persister`最初保存Raft最近的持久化状态（如果有的话）。Raft应该从该`Persister`初始化其状态，并在每次状态变化时使用它保存持久化状态。使用`Persister`的`ReadRaftState()`和`Save()`方法。

### 任务
完成`raft.go`中的`persist()`和`readPersist()`函数，添加代码以保存和恢复持久化状态。你需要将状态编码（或“序列化”）为字节数组以传递给`Persister`。使用`labgob`编码器；见`persist()`和`readPersist()`中的注释。`labgob`类似于Go的`gob`编码器，但如果你尝试编码小写字段名的结构，它会打印错误消息。现在，将`nil`作为第二个参数传递给`persister.Save()`。在实现更改持久化状态的地方插入对`persist()`的调用。一旦你完成这些，并且实现的其余部分是正确的，你应该通过所有的3C测试。

你可能需要通过一次退回多个条目来优化`nextIndex`。从论文第7页底部和第8页顶部（用灰线标记）开始查看扩展的Raft论文。论文对细节不明确；你需要填补这些空白。一种可能性是让拒绝消息包括：

```
XTerm:  冲突条目的任期（如果有的话）
XIndex: 具有该任期的第一个条目的索引（如果有的话）
XLen:   日志长度
```

然后领导者的逻辑可以是这样的：

```
情况1：领导者没有XTerm：
  nextIndex = XIndex
情况2：领导者有XTerm：
  nextIndex = 领导者最后一个XTerm的条目
情况3：跟随者的日志太短：
  nextIndex = XLen
```

### 其他提示
- 运行`git pull`获取最新的实验软件。
- 3C测试比3A或3B的要求更高，失败可能是由于3A或3B代码中的问题。
- 你的代码应该通过所有3C测试（如下所示），以及3A和3B测试。

```bash
$ go test -run 3C
Test (3C): basic persistence ...
  ... Passed --   5.0  3   86   22849    6
Test (3C): more persistence ...
  ... Passed --  17.6  5  952  218854   16
Test (3C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   2.0  3   34    8937    4
Test (3C): Figure 8 ...
  ... Passed --  31.2  5  580  130675   32
Test (3C): unreliable agreement ...
  ... Passed --   1.7  5 1044  366392  246
Test (3C): Figure 8 (unreliable) ...
  ... Passed --  33.6  5 10700 33695245  308
Test (3C): churn ...
  ... Passed --  16.1  5 8864 44771259 1544
Test (3C): unreliable churn ...
  ... Passed --  16.5  5 4220 6414632  906
PASS
ok      6.5840/raft    123.564s
$
```

在提交之前多次运行测试，确保每次运行都打印PASS是个好主意。

```bash
$ for i in {0..10}; do go test; done
```

## Part 3D：日志压缩（难度大）

### 任务
目前为止，重启的服务器通过重放完整的Raft日志来恢复其状态。然而，对于长时间运行的服务来说，记住完整的Raft日志是不实际的。相反，你将修改Raft，以配合持久存储其状态快照的服务，在某些时候Raft会丢弃快照之前的日志条目。结果是更少的持久化数据和更快的重启。然而，这也可能导致跟随者落后于领导者，而领导者已经丢弃了跟随者需要赶上的日志条目；此时，领导者必须发送一个快照和从快照开始的日志。扩展Raft论文的第7节概述了这个方案；你需要设计细节。

你的Raft必须提供以下函数，服务可以调用该函数来传递其状态的序列化快照：

```go
Snapshot(index int, snapshot []byte)
```

在实验3D中，测试器会定期调用`Snapshot()`。在实验4中，你将编写一个调用`Snapshot()`的键/值服务器；快照将包含键/值对的完整表。服务层会在每个节点上调用`Snapshot()`（不仅仅是领导者）。

`index`参数表示反映在快照中的最高日志条目。Raft应丢弃该点之前的日志条目。你需要修改Raft代码，使其在仅存储日志尾部的情况下工作。

你需要实现论文中讨论的`InstallSnapshot` RPC，这允许Raft领导者通知落后的Raft节点用快照替换其状态。你可能需要仔细考虑`InstallSnapshot`如何与图2中的状态和规则进行交互。

当跟随者的Raft代码收到`InstallSnapshot` RPC时，它可以使用`applyCh`将快照发送到服务中的`ApplyMsg`。`ApplyMsg`结构体定义已经包含了你需要的字段（并且测试器期望这些字段）。确保这些快照仅推进服务的状态，而不会导致状态倒退。

如果服务器崩溃，它必须从持久化数据重新启动。你的Raft应持久化Raft状态和相应的快照。使用`persister.Save()`的第二个参数保存快照。如果没有快照，传递`nil`作为第二个参数。

当服务器重新启动时，应用层读取持久化的快照并恢复其保存的状态。

### 任务
实现`Snapshot()`和`InstallSnapshot` RPC，以及支持这些功能所需的Raft修改（例如，操作修剪后的日志）。当你通过3D测试（以及所有之前的实验3测试）时，你的解决方案就完成了。

### 提示
- 运行`git pull`确保你有最新的软件。
- 一个好的起点是修改代码，使其能够存储从某个索引X开始的日志部分。最初你可以将X设置为零并运行3B/3C测试。然后让`Snapshot(index)`丢弃index之前的日志，并将X设置为index。如果一切顺利，你现在应该通过第一个3D测试。
- 接下来：如果领导者没有跟随者所需的日志条目来使其赶上，则发送`InstallSnapshot` RPC。
- 在单个`InstallSnapshot` RPC中发送整个快照。不要实现图13中分割快照的偏移机制。
- Raft必须以允许Go垃圾收集器释放和重用内存的方式丢弃旧日志条目；这要求没有可达引用（指针）指向被丢弃的日志条目。
- 实验3的所有测试（3A+3B+3C+3D）在不带`-race`的情况下，消耗的合理时间是6分钟的实际时间和1分钟的CPU时间。使用`-race`运行时，大约是10分钟的实际时间和2分钟的CPU时间。
- 你的代码应该通过所有3D测试（如下所示），以及3A、3B和3C测试。

```bash
$ go test -run 3D
Test (3D): snapshots basic ...
  ... Passed --  11.6  3  176   61716  192
Test (3D): install snapshots (disconnect) ...
  ... Passed --  64.2  3  878  320610  336
Test (3D): install snapshots (disconnect+unreliable) ...
  ... Passed --  81.1  3 1059  375850  341
Test (3D): install snapshots (crash) ...
  ... Passed --  53.5  3  601  256638  339
Test (3D): install snapshots (unreliable+crash) ...
  ... Passed --  63.5  3  687  288294  336
Test (3D): crash and restart all servers ...
  ... Passed --  19.5  3  268   81352   58
PASS
ok      6.5840/raft      293.456s
```
在提交之前多次运行测试，确保每次运行都打印PASS是个好主意。
