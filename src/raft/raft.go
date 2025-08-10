package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	workerStatus       *WorkerStatus // 工作状态，Follower, Candidate, Leader
	currentTerm        atomic.Int64  // 当前任期
	votedFor           atomic.Int64  // 当前任期内投票给了哪个candidate，-1表示未投票
	electionResetEvent chan struct{} // 计时器刷新事件
	// 3B
	commitIndex  atomic.Int64 // 已提交的日志索引
	lastApplied  atomic.Int64 // 最后应用到状态机的日志索引
	logEntries   LogEntries   // 日志条目，包含命令和任期
	lastLogIndex atomic.Int64 // 最后一条日志的索引，初始为0,防止对len的访问竞态
	// 3B for leader
	nextIndex  []int // 对于每个follower，leader期望的下一个日志索引
	matchIndex []int // 对于每个follower，leader已知的已提交日志索引
	applyCh    chan ApplyMsg

	// 3C advanced Raft
	// 提交通知（事件驱动的异步 apply）
	commitNotify chan struct{}
	// [forbid] appendEntries单飞 防止leader在发送AppendEntries过快
	//appendEntriesRequesting []int32

	// 3D snapshot
	lastIncludedIndex int64 // 快照包含的最后一条日志的索引
	lastIncludedTerm  int64 // 快照包含的最后一条日志的任期
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds) // 设置日志格式为毫秒
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.workerStatus = NewWorkerStatusFromInt(FollowerStatus)
	rf.currentTerm.Store(0)
	rf.votedFor.Store(-1) // -1 表示未vote
	rf.electionResetEvent = make(chan struct{}, 1)
	// 3B
	rf.commitIndex.Store(0)
	rf.lastApplied.Store(0)
	rf.logEntries = make(LogEntries, 0)
	// [重要]初始化一个空日志，防止出现对初始值的处理
	rf.logEntries.Append([]Entry{
		{
			Term:    0,   // 初始日志条目，任期为0
			Command: nil, // 初始日志条目没有命令
		},
	})
	rf.lastLogIndex.Store(0) // 初始日志索引为0，代表空日志
	rf.applyCh = applyCh
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)   // 初始时，leader期望每个follower的下一个日志索引为1
		rf.matchIndex = append(rf.matchIndex, 0) // 初始时，leader已知的已提交日志索引为0
	}

	// 事件驱动提交信号
	rf.commitNotify = make(chan struct{}, 1)
	// appendEntries单飞，防止leader在发送AppendEntries过快
	//rf.appendEntriesRequesting = make([]int32, len(rf.peers))
	// initialize from state persisted before a crash
	rf.lastIncludedIndex = 0 // 初始快照索引为0
	rf.lastIncludedTerm = 0  // 初始快照任期为0
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.committer()

	return rf
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	term = int(rf.currentTerm.Load())
	isleader = rf.workerStatus.value.Load() == LeaderStatus

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm.Load())
	e.Encode(rf.votedFor.Load())
	e.Encode(rf.logEntries)
	e.Encode(rf.lastLogIndex.Load())
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logEntries LogEntries
	var currentTerm int64
	var votedFor int64
	var lastLogIndex int64
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil ||
		d.Decode(&lastLogIndex) != nil {
		DPrintf("[ERROR][readPersist] decode error")
	} else {
		rf.currentTerm.Store(currentTerm)
		rf.votedFor.Store(votedFor)
		rf.logEntries = logEntries
		rf.lastLogIndex.Store(lastLogIndex)
		DPrintf("[readPersist] currentTerm %d, votedFor %d, lastLogIndex %d, logEntries %v", rf.currentTerm.Load(), rf.votedFor.Load(), rf.lastLogIndex.Load(), rf.logEntries)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int64 // candidate's term
	CandidateId int   // candidate requesting vote
	// 3B
	LastLogIndex int64 // Candidate最后一条日志记录的索引
	LastLogTerm  int64 // Candidate最后一条日志记录的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int64 // currentTerm, for a candidate to update itself
	VoteGranted bool  // true 表示server向client投票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// client的任期小于server，直接返回
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm.Load() {
		reply.Term = rf.currentTerm.Load()
		reply.VoteGranted = false
		return
		// server过期，无论怎样都变为follower
	} else if args.Term > rf.currentTerm.Load() {
		rf.becomeFollower(args.Term)
	}
	// 检测是否可以投票给candidate
	voteFor := rf.votedFor.Load()
	if voteFor == -1 || voteFor == int64(args.CandidateId) {
		// 检查candidate的日志是否比up-to-date
		lastLogTerm := rf.logEntries.GetLast().Term
		lastLogIndex := rf.lastLogIndex.Load() // 最后一条日志的索引
		isUpToDate := (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
		if isUpToDate {
			rf.resetElectionTimer()
			rf.votedFor.Store(int64(args.CandidateId))
			// votedFor变化，persist
			rf.persist()
			reply.VoteGranted = true
			DPrintf("[RequestVote] %d vote for %d, term %d", rf.me, args.CandidateId, args.Term)
		} else {
			reply.VoteGranted = false
			DPrintf("[RequestVote] %d reject vote for %d,expected log term %d, index %d, but got %d, %d", rf.me, args.CandidateId, lastLogTerm, lastLogIndex, args.LastLogTerm, args.LastLogIndex)
		}
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm.Load()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term     int64 // leader's term
	LeaderId int   // so follower can redirect clients
	//CommitIndex int64 // leader's commitIndex, 用于follower更新自己的commitIndex(心跳时使用)
	// 3B
	AppendArgs *AppendEntriesAppendArgs // 用于追加日志条目
}

// AppendEntriesAppendArgs LogEntries相关的args
type AppendEntriesAppendArgs struct {
	PrevLogIndex int64 // 前继日志记录的索引
	PrevLogTerm  int64 // 前继日志记录的任期
	Entries      LogEntries
	CommitIndex  int64 // leader's commitIndex, 用于follower更新自己的commitIndex
}
type AppendEntriesReply struct {
	Term          int64 // currentTerm, for leader to update itself
	Success       bool  // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictTerm  int64 // follower 日志的冲突 term，-1 表示缺少条目
	ConflictIndex int64 // follower 希望 leader 退回到的 nextIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// leader的任期小于server
	if args.Term < rf.currentTerm.Load() {
		reply.Term = rf.currentTerm.Load()
		reply.Success = false
		return
	}
	// 收到更大任期或来自 leader 的心跳/追加
	rf.resetElectionTimer()
	// 只要收到了新leader的心跳，就将自己变为follower
	if args.Term > rf.currentTerm.Load() || rf.workerStatus.value.Load() != FollowerStatus {
		rf.becomeFollower(args.Term)
	}
	// 如果自己不存在索引、任期和 prevLogIndex、prevLogItem 匹配的日志返回 false

	reply.Term = rf.currentTerm.Load()

	prevIndex := args.AppendArgs.PrevLogIndex
	prevTerm := args.AppendArgs.PrevLogTerm
	prevEntry := rf.Find3D(prevIndex)

	// 统一提供快速回退所需的冲突信息（包括心跳路径）
	if prevEntry == nil {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastLogIndex.Load() + 1
		return
	}
	if prevEntry.Term != prevTerm {
		// term 冲突：找到 follower 本地该 term 的第一位置
		conflictTerm := prevEntry.Term
		first := prevIndex
		for first > 0 && rf.Find3D(first-1).Term == conflictTerm {
			first--
		}
		reply.Success = false
		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = first
		return
	}

	// 走到这里 prev 一定匹配
	entries := args.AppendArgs.Entries
	if entries == nil || len(entries) == 0 {
		// 心跳：仅推进 commitIndex
		reply.Success = true
		if args.AppendArgs.CommitIndex > rf.commitIndex.Load() {
			rf.commitIndex.Store(minI64(rf.lastLogIndex.Load(), args.AppendArgs.CommitIndex))
			// commitIndex更新，触发event
			rf.notifyCommit()
		}
		return
	}

	// 追加：按 RFC 的做法，只在发现冲突处截断，然后附加后缀
	// 从 prevIndex+1 开始，对齐 entries
	appendFrom := int64(len(entries)) // 默认都匹配（即只需要可能扩展）
	for k := int64(0); k < int64(len(entries)); k++ {
		idx := prevIndex + 1 + k
		ex := rf.Find3D(idx)
		if ex == nil {
			appendFrom = k
			break
		}
		if ex.Term != entries[k].Term {
			// 截断到冲突点（不包含 idx）
			rf.ReservePrefix3D(idx - 1)
			rf.lastLogIndex.Store(idx - 1)
			appendFrom = k
			break
		}
	}
	if appendFrom < int64(len(entries)) {
		rf.logEntries.Append(entries[appendFrom:])
		rf.SetLastLogIndex3D()
		rf.persist()
	}

	// 推进 commitIndex
	if args.AppendArgs.CommitIndex > rf.commitIndex.Load() {
		rf.commitIndex.Store(minI64(rf.lastLogIndex.Load(), args.AppendArgs.CommitIndex))
		// figure8(unreliable): 发送event触发提交
		rf.notifyCommit()
	}

	reply.Term = rf.currentTerm.Load()
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int64  // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int64  // 快照包含的最后一条日志的索引
	LastIncludedTerm  int64  // 快照包含的最后一条日志的任期
	Offset            int64  // 快照数据的偏移量
	Data              []byte // 快照数据
	Done              bool   // 是否是最后一块数据
}
type InstallSnapshotReply struct {
	Term    int64 // currentTerm, for leader to update itself
	Success bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果 leader 的任期小于自己的任期返回 false
	if args.Term < rf.currentTerm.Load() {
		reply.Term = rf.currentTerm.Load()
		reply.Success = false
		return
	}
	// 收到更大任期或来自 leader 的心跳/追加
	rf.resetElectionTimer()
	// 只要收到了新leader的心跳，就将自己变为follower
	if args.Term > rf.currentTerm.Load() || rf.workerStatus.value.Load() != FollowerStatus {
		rf.becomeFollower(args.Term)
	}
	// 在快照文件的指定偏移量写入数据

}

// sendInstallSnapshot sends InstallSnapshot RPC to a server.
// 当Follower刚刚恢复，如果它的Log短于Leader通过 AppendEntries RPC发给它的内容，
// 那么它首先会强制Leader回退自己的Log。在某个点，Leader将不能再回退,这时，Leader会将自己的快照发给Follower
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//第一个返回值是该命令如果被提交后将出现在日志中的索引
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	if rf.killed() {
		return index, term, false
	}
	if !rf.workerStatus.Equal(LeaderStatus) {
		return index, int(rf.currentTerm.Load()), false
	}
	rf.mu.Lock()
	currentTerm := rf.currentTerm.Load()
	newEntry := Entry{
		Term:    currentTerm,
		Command: command,
	}
	rf.logEntries = append(rf.logEntries, newEntry)
	rf.lastLogIndex.Add(1) // 更新最后一条日志的索引
	// 日志变化，persist
	rf.persist()
	index = int(rf.lastLogIndex.Load())
	term = int(rf.currentTerm.Load())
	// 更新leader的nextIndex和matchIndex，虽然好像不太重要
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.mu.Unlock()

	// 立刻触发一次发送，加快复制
	go rf.sendHeartbeatsAndLogEntries()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// resetElectionTimer 重置计时器
func (rf *Raft) resetElectionTimer() {
	select {
	case rf.electionResetEvent <- struct{}{}:
	default:
	}
}

func (rf *Raft) ticker() {
	electionTimeout := func() time.Duration {
		return time.Duration(150+rand.Intn(200)) * time.Millisecond
	}

	for !rf.killed() {
		timeout := electionTimeout()
		timer := time.NewTimer(timeout)
		select {
		case <-timer.C: // 选举超时
			rf.startElection()
		case <-rf.electionResetEvent: // 收到心跳或投票
			timer.Stop()
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.becomeCandidate()
	currentTerm := rf.currentTerm.Load()
	rf.mu.Unlock()

	var votes atomic.Int32
	votes.Add(1) // candidate自己投票给自己
	DPrintf("[RequestVote] %d vote for itself, term %d", rf.me, currentTerm)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &RequestVoteArgs{
			Term:        currentTerm,
			CandidateId: rf.me,
			// 3B
			LastLogIndex: rf.lastLogIndex.Load(),       // Candidate最后一条日志记录的索引
			LastLogTerm:  rf.logEntries.GetLast().Term, // Candidate最后一条日志记录的任期
		}

		go func(peer int) {
			var reply RequestVoteReply
			if rf.sendRequestVote(peer, args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// 条件1是变为leader或降级为follower
				// 这里不需要强一致，最终投票的follower都会再次票给新leader
				if !rf.workerStatus.Equal(CandidateStatus) || rf.currentTerm.Load() != currentTerm {
					return // 已经变成 follower 或 term 变了
				}
				if reply.Term > currentTerm {
					rf.becomeFollower(reply.Term)
					return
				}
				if reply.VoteGranted {
					if votes.Add(1) > int32(len(rf.peers)/2) {
						rf.becomeLeader()
					}
				}
			} else {
				// 网络问题，不处理
			}
		}(i)
	}
}

func (rf *Raft) sendHeartbeatsAndLogEntries() {
	// [forbid code]
	//rf.mu.Lock()
	//currentTerm := rf.currentTerm.Load()
	//lastLogIndex := rf.lastLogIndex.Load()
	//commitIndex := rf.commitIndex.Load()
	//rf.mu.Unlock()
	// 为避免读取不一致，针对每个 peer 单独快照当前需要发送的 prev/entries/commit
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// 同时只允许一个requesting
		//if !atomic.CompareAndSwapInt32(&rf.appendEntriesRequesting[i], 0, 1) {
		//	continue
		//}

		rf.mu.Lock()
		if !rf.workerStatus.Equal(LeaderStatus) {
			rf.mu.Unlock()
			return
		}
		term := rf.currentTerm.Load()
		next := rf.nextIndex[i]
		prevIndex := int64(next - 1)
		prevTerm := rf.Find3D(prevIndex).Term
		lastIndex := rf.lastLogIndex.Load()
		commitIdx := rf.commitIndex.Load()

		var entries LogEntries
		if int64(next) <= lastIndex {
			entries = rf.SplitEntriesFrom3D(int64(next))
		} else {
			entries = nil
		}
		// figure8(unreliable)：取消每次AppendEntries时的leader状态一致性，有新日志就发送出去
		args := &AppendEntriesArgs{Term: term, LeaderId: rf.me,
			AppendArgs: &AppendEntriesAppendArgs{PrevLogIndex: prevIndex, PrevLogTerm: prevTerm, Entries: entries, CommitIndex: commitIdx}}
		sentPrev := prevIndex
		sentCount := 0
		if entries != nil {
			sentCount = len(entries)
		}
		rf.mu.Unlock()

		go func(peer int, a *AppendEntriesArgs, sentPrev int64, sentCount int) {
			//defer atomic.StoreInt32(&rf.appendEntriesRequesting[peer], 0)
			var reply AppendEntriesReply
			if rf.sendAppendEntries(peer, a, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm.Load() {
					rf.becomeFollower(reply.Term)
					return
				}
				if !rf.workerStatus.Equal(LeaderStatus) || rf.currentTerm.Load() != a.Term {
					return
				}

				if reply.Success {
					// figure8(unreliable)：新增单调推进，避免旧回复回退 nextIndex
					newNext := int(sentPrev) + sentCount + 1
					if rf.nextIndex[peer] < newNext {
						rf.nextIndex[peer] = newNext
						rf.matchIndex[peer] = maxInt(rf.matchIndex[peer], newNext-1)
						rf.leaderCheckCommitIndex()
						// 取得进展，立刻再发一轮，加快追赶
						//go rf.sendHeartbeatsAndLogEntries()
					}
				} else {
					// figure8(unreliable)：快速回退resp
					var candidateNext int
					if reply.ConflictTerm == -1 {
						candidateNext = int(reply.ConflictIndex)
					} else {
						// 在 leader 日志中找该 term 的最后一次出现
						last := int(rf.lastLogIndex.Load())
						j := 0
						for idx := last; idx > 0; idx-- {
							if rf.Find3D(int64(idx)).Term == reply.ConflictTerm {
								j = idx
								break
							}
						}
						if j > 0 {
							candidateNext = j + 1
						} else {
							candidateNext = int(reply.ConflictIndex)
						}
					}
					if candidateNext < rf.nextIndex[peer] {
						rf.nextIndex[peer] = candidateNext
						// 产生回退，立刻重发
						go rf.sendHeartbeatsAndLogEntries()
					}
				}
			}
		}(i, args, sentPrev, sentCount)
	}

	// 重置选举定时器，防止leader心跳过期
	rf.resetElectionTimer()
}

// leaderCheckCommitIndex [上层有锁]检查leader的commitIndex
func (rf *Raft) leaderCheckCommitIndex() {
	// 3C
	old := rf.commitIndex.Load()
	newCommit := old
	//for N := rf.lastLogIndex.Load(); N > rf.commitIndex.Load() &&
	for N := rf.lastLogIndex.Load(); N > rf.commitIndex.Load(); N-- {
		if rf.Find3D(N).Term != rf.currentTerm.Load() {
			continue // 跳过，而非退出，之前的日志可能一致
		}
		count := 1
		for s, matchIndex := range rf.matchIndex {
			if s == rf.me {
				continue
			}
			if int64(matchIndex) >= N {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			newCommit = N
		}
	}
	if newCommit > old {
		rf.commitIndex.Store(newCommit)
		//  figure8(unreliable)：手动触发提交event
		rf.notifyCommit()
	}
	DPrintf("[LeaderCheckCommitIndex] %d commitIndex %d -> %d, lastLogIndex %d", rf.me, old, rf.commitIndex.Load(), rf.lastLogIndex.Load())
}

// committer figure8(unreliable)：事件驱动 + 定时兜底；一轮内清空新commit
func (rf *Raft) committer() {
	commitTimeout := func() time.Duration { return time.Duration(10+rand.Intn(50)) * time.Millisecond }
	for !rf.killed() {
		timer := time.NewTimer(commitTimeout())
		select {
		case <-timer.C:
			// 退出select，进入commit流程
		case <-rf.commitNotify:
			if !timer.Stop() {
				<-timer.C
			}
		}
		// 尽量在同一轮里把 backlog 都 apply 掉
		for {
			rf.mu.Lock()
			lastApplied := rf.lastApplied.Load()
			commitIndex := rf.commitIndex.Load()

			// 有快照还没交付给状态机，先下发快照
			if lastApplied < rf.lastIncludedIndex {
				snap := rf.persister.ReadSnapshot() // 3D 时持久化的快照
				snapIdx := rf.lastIncludedIndex
				snapTerm := rf.lastIncludedTerm
				rf.mu.Unlock()

				rf.applyCh <- ApplyMsg{
					CommandValid:  false,
					SnapshotValid: true,
					Snapshot:      snap,
					SnapshotIndex: int(snapIdx),
					SnapshotTerm:  int(snapTerm),
				}
				// 快照已应用，推进 lastApplied；下一轮再继续 apply 日志
				rf.lastApplied.Store(snapIdx)
				continue
			}

			// 没有可 apply 的新日志
			if commitIndex <= lastApplied {
				rf.mu.Unlock()
				break
			}

			DPrintf("[committer] committer %d, lastApplied %d, commitIndex %d", rf.me, lastApplied, commitIndex)
			// 计算要 apply 的绝对区间 [startAbs, endAbs]
			startAbs := lastApplied + 1
			endAbs := commitIndex

			// 映射到切片下标
			sIdx, okS := rf.toSliceIndex(startAbs) // 期望 ≥ 1（0 是哨兵）
			eIdx, okE := rf.toSliceIndex(endAbs)
			if !okS || !okE || sIdx > eIdx {
				// 理论上不该发生
				rf.mu.Unlock()
				break
			}

			// 拷贝要 apply 的日志到独立切片
			todo := make(LogEntries, eIdx-sIdx+1)
			copy(todo, rf.logEntries[sIdx:eIdx+1])
			rf.mu.Unlock()

			// 应用日志条目到状态机（无锁）
			for i, entry := range todo {
				absIndex := int(startAbs) + i
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: absIndex,
				}
				rf.lastApplied.Add(1)
			}

		}
	}
}

// 发出一次提交的通知（非阻塞）
func (rf *Raft) notifyCommit() {
	select {
	case rf.commitNotify <- struct{}{}:
	default:
	}
}
