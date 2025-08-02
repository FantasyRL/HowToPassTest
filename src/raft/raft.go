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
	"log"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
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

	// initialize from state persisted before a crash
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
	//if isleader {
	//	DPrintf("[GetState] current term is %d\n", term)
	//}
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
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
	Term        int64 // currentTerm, for candidate to update itself
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
	Term    int64 // currentTerm, for leader to update itself
	Success bool  // true if follower contained entry matching prevLogIndex and prevLogTerm
	//ConflictTerm  int64 // follower 日志的冲突 term，-1 表示缺少条目
	//ConflictIndex int64 // follower 希望 leader 退回到的 nextIndex
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
	rf.resetElectionTimer()
	// 只要收到了新leader的心跳，就将自己变为follower
	if args.Term > rf.currentTerm.Load() || rf.workerStatus.value.Load() != FollowerStatus {
		rf.becomeFollower(args.Term)
	}
	// 如果自己不存在索引、任期和 prevLogIndex、prevLogItem 匹配的日志返回 false

	// 如果没有日志，则是心跳包，尝试更新commitIndex，若返回false，说明日志未匹配上
	if args.AppendArgs.Entries == nil {
		reply.Term = rf.currentTerm.Load()
		logEntry := rf.logEntries.Find(args.AppendArgs.PrevLogIndex)
		// term未匹配，说明日志是需要丢弃的
		if logEntry == nil || logEntry.Term != args.AppendArgs.PrevLogTerm {
			reply.Success = false
		} else {
			reply.Success = true
			// 更新commitIndex
			if args.AppendArgs.CommitIndex > rf.commitIndex.Load() {
				rf.commitIndex.Store(minI64(rf.lastLogIndex.Load(), args.AppendArgs.CommitIndex))
			}
		}
		return
	}
	entry := rf.logEntries.Find(args.AppendArgs.PrevLogIndex)
	// 不存在这个日志，说明缺日志，要更往前的日志
	if entry == nil {
		reply.Term = rf.currentTerm.Load()
		reply.Success = false
		//reply.ConflictTerm = -1
		//reply.ConflictIndex = rf.lastLogIndex.Load() + 1 // 让leader知道要回退到哪里
		return
	}
	// 如果存在这个日志，但是任期不匹配，说明日志不一致
	if entry.Term != args.AppendArgs.PrevLogTerm {
		// 删除这条日志及所有后继日志,和leader拿更往前的日志来校验
		rf.logEntries, _ = rf.logEntries.Split(args.AppendArgs.PrevLogIndex)
		rf.lastLogIndex.Store(args.AppendArgs.PrevLogIndex - 1)
		//conflictTerm := entry.Term
		// 告诉leader拿当前冲突term之前一个term的日志索引
		//first := args.AppendArgs.PrevLogIndex
		//for first > 0 && rf.logEntries.Find(first-1).Term == conflictTerm {
		//	first--
		//}
		reply.Term = rf.currentTerm.Load()
		reply.Success = false
		//reply.ConflictTerm = conflictTerm
		//reply.ConflictIndex = first // 让leader知道下次的nextIndex
		return
	}
	// 如果存在这个日志，且任期匹配，说明日志一致,追加日志
	rf.logEntries, _ = rf.logEntries.Split(args.AppendArgs.PrevLogIndex + 1)
	rf.logEntries.Append(args.AppendArgs.Entries)
	rf.lastLogIndex.Store(int64(rf.logEntries.Len() - 1))

	rf.commitIndex.Store(minI64(rf.lastLogIndex.Load(), args.AppendArgs.CommitIndex))
	DPrintf("[AppendEntries] %d append entries from %d, term %d, prevIndex %d, prevTerm %d, entries %v", rf.me, args.LeaderId, args.Term, args.AppendArgs.PrevLogIndex, args.AppendArgs.PrevLogTerm, args.AppendArgs.Entries)

	reply.Term = rf.currentTerm.Load()
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index = int(rf.lastLogIndex.Load())
	term = int(rf.currentTerm.Load())
	// 更新leader的nextIndex和matchIndex，虽然好像不太重要
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.mu.Unlock()

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
	rf.mu.Lock()
	currentTerm := rf.currentTerm.Load()
	lastLogIndex := rf.lastLogIndex.Load()
	commitIndex := rf.commitIndex.Load()
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &AppendEntriesArgs{
			Term:     currentTerm,
			LeaderId: rf.me,
		}
		// todo: 这里可能会有问题，我是在确保新增日志只会在下个心跳周期内追加
		// 加锁，保护对日志的访问
		rf.mu.Lock()
		nextIndex := rf.nextIndex[i]
		prevIndex := int64(nextIndex - 1)
		prevTerm := rf.logEntries.Find(prevIndex).Term
		entries := LogEntries(nil)
		if int64(nextIndex) <= lastLogIndex {
			entries = make(LogEntries, len(rf.logEntries[nextIndex:]))
			copy(entries, rf.logEntries[nextIndex:])
			//DPrintf("[SendAppendEntries] %d send to %d, term %d, nextIndex %d, prevIndex %d, prevTerm %d, entries %v", rf.me, i, currentTerm, nextIndex, prevIndex, prevTerm, entries)
		}
		args.AppendArgs = &AppendEntriesAppendArgs{
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			CommitIndex:  commitIndex,
		}
		rf.mu.Unlock()

		go func(peer int) {
			var reply AppendEntriesReply
			if rf.sendAppendEntries(peer, args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm.Load() {
					rf.becomeFollower(reply.Term)
				}
				if !rf.workerStatus.Equal(LeaderStatus) || rf.currentTerm.Load() != currentTerm {
					return // 已经变成 follower 或 term 变了
				}
				// 心跳包的replySuccess不处理
				if reply.Success && args.AppendArgs.Entries != nil {
					rf.nextIndex[peer] = nextIndex + len(args.AppendArgs.Entries)
					rf.matchIndex[peer] = rf.nextIndex[peer] - 1
					rf.leaderCheckCommitIndex()
				} else if !reply.Success {
					rf.nextIndex[peer] = maxInt(rf.nextIndex[peer]-1, 1)
					//if reply.ConflictTerm == -1 { // follower 日志太短
					//	rf.nextIndex[peer] = int(reply.ConflictIndex)
					//} else {
					//	// 在自己日志中寻找 ConflictTerm 最后一条
					//	idx := rf.lastLogIndex.Load()
					//	for idx > 0 && rf.logEntries.Find(idx).Term != reply.ConflictTerm {
					//		idx--
					//	}
					//	if idx > 0 { // 找到了同 term
					//		rf.nextIndex[peer] = int(idx + 1)
					//	} else { // 找不到，同论文第二条策略
					//		rf.nextIndex[peer] = int(reply.ConflictIndex)
					//	}
					//}
				}
			}
		}(i)
	}
	// 重置选举定时器，防止leader心跳过期
	rf.resetElectionTimer()
}

// leaderCheckCommitIndex [上层有锁]检查leader的commitIndex
func (rf *Raft) leaderCheckCommitIndex() {
	for N := rf.lastLogIndex.Load(); N > rf.commitIndex.Load() &&
		rf.logEntries.Find(N).Term == rf.currentTerm.Load(); N-- {
		count := 1
		for s, matchIndex := range rf.matchIndex {
			if s == rf.me {
				continue
			}
			if int64(matchIndex) >= N {
				count++
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex.Store(N)
				break
			}
		}
	}
	DPrintf("[LeaderCheckCommitIndex] %d check commit index %d, lastLogIndex %d", rf.me, rf.commitIndex.Load(), rf.lastLogIndex.Load())
}

func (rf *Raft) committer() {
	commitTimeout := func() time.Duration {
		return time.Duration(50+rand.Intn(50)) * time.Millisecond
	}

	for !rf.killed() {
		timeout := commitTimeout()
		timer := time.NewTimer(timeout)
		select {
		case <-timer.C:
			rf.mu.Lock()
			lastApplied := rf.lastApplied.Load()
			commitIndex := rf.commitIndex.Load()
			if lastApplied >= commitIndex { // 没有新的日志需要应用
				rf.mu.Unlock()
				timer.Stop()
				continue
			}

			// 复制需要 apply 的条目到独立切片
			todo := make(LogEntries, commitIndex-lastApplied)
			copy(todo, rf.logEntries[lastApplied+1:commitIndex+1])

			// 先别更新 lastApplied；等真正推送完再写
			rf.mu.Unlock()

			DPrintf("[Committer] %d commit entries from %d to %d, entries %v",
				rf.me, lastApplied+1, commitIndex, todo)

			// 应用日志条目到状态机（无锁）
			for i, entry := range todo {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: int(lastApplied) + i + 1,
				}
				DPrintf("[Apply] %d apply log %v", rf.me, entry)
				rf.applyCh <- msg
				rf.lastApplied.Add(1)
			}
		}
	}
}
