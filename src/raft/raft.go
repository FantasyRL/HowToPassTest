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
	isVotedFor         atomic.Bool   // 本任期中是否已经投票
	isReceiveHeartBeat atomic.Bool   // 是否接收到leader的心跳
	voteNums           atomic.Int64  // Candidate当前投票数，candidate需要大于等于len(peers)/2+1才能成为leader
	electionResetEvent chan struct{} // 选举重置事件
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
	rf.isVotedFor.Store(false)
	rf.voteNums.Store(0)
	rf.isReceiveHeartBeat.Store(false)
	rf.electionResetEvent = make(chan struct{}, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

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
		rf.votedFor.Store(int64(args.CandidateId))
		reply.VoteGranted = true
		DPrintf("[RequestVote] %d vote for %d, term %d", rf.me, args.CandidateId, args.Term)
		rf.resetElectionTimer()
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
}
type AppendEntriesReply struct {
	Term    int64 // currentTerm, for leader to update itself
	Success bool  // true if follower contained entry matching prevLogIndex and prevLogTerm
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
	// 只要收到了新leader的心跳，就将自己变为follower
	rf.resetElectionTimer()
	if args.Term > rf.currentTerm.Load() || rf.workerStatus.value.Load() != FollowerStatus {
		rf.becomeFollower(args.Term)
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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
		args := &RequestVoteArgs{Term: currentTerm, CandidateId: rf.me}

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

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	term := rf.currentTerm.Load()
	rf.mu.Unlock()

	args := &AppendEntriesArgs{
		Term:     term,
		LeaderId: rf.me,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			var reply AppendEntriesReply
			if rf.sendAppendEntries(peer, args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm.Load() {
					rf.becomeFollower(reply.Term)
				}
			}
		}(i)
	}
	// 重置选举定时器，防止leader心跳过期
	rf.resetElectionTimer()
}
