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
	"bytes"
	"github.com/sasha-s/go-deadlock"
	//	"bytes"
	"math/rand"
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
	mu        deadlock.Mutex      // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int64               // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//3A
	currentTerm   int64
	votedFor      int64
	status        int32
	LastHeartBeat time.Time
	//LastLogCome   chan bool
	//ElectionSync sync.WaitGroup //原来这玩意没用？？？？
	//3B
	log         LogEntries
	commitIndex int64
	lastApplied int64
	NextIndex   []int64
	MatchIndex  []int64
	//AppendEntriesSync sync.WaitGroup
}

const (
	Follower = iota
	Candidate
	Leader
)

type Entry struct {
	Term    int64
	Command interface{} //machine command
	//Index   int64
}
type LogEntries []Entry

// get entry
func (logEntries LogEntries) getEntryByIndex(index int64) *Entry {
	if index <= 0 {
		return &Entry{
			Command: nil,
			Term:    0,
		}
	}
	if index > int64(len(logEntries)) {
		return &Entry{
			Command: nil,
			Term:    -1,
		}
	}
	return &logEntries[index-1]
}

// get slice
func (logEntries LogEntries) getEntrySlice(startIndex, endIndex int64) LogEntries {
	if startIndex <= 0 {
		startIndex = 1
	}
	if startIndex > endIndex { //防报错
		endIndex = startIndex
	}
	return logEntries[startIndex-1 : endIndex-1] //防报错
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = int(atomic.LoadInt64(&rf.currentTerm))
	//DPrintf("%v", len(rf.peers))
	if atomic.LoadInt32(&rf.status) == Leader {
		isleader = true
	}
	rf.mu.Unlock()
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	//e.Encode(rf.commitIndex)
	//e.Encode(rf.lastApplied)
	//e.Encode(rf.MatchIndex)
	//e.Encode(rf.NextIndex)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
	DPrintf("encode success")
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	//var xxx
	// var yyy
	//if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var log LogEntries
	var currentTerm int64
	var votedFor int64
	//var commitIndex int64
	//var lastApplied int64
	//var NextIndex []int64
	//var MatchIndex []int64
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		DPrintf("%v:decode error:\n%v\n", rf.me, d.Decode(&log))
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		//rf.commitIndex = commitIndex
		//rf.lastApplied = lastApplied
		//rf.NextIndex = NextIndex
		//rf.MatchIndex = MatchIndex
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int64 //候选者任期
	CandidateId  int64
	LastLogIndex int64
	LastLogTerm  int64
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int64
	VoteGranted bool
	IsExpired   bool
	IsKilled    bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	reply.IsKilled = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		reply.VoteGranted = false
		reply.IsExpired = false
		reply.IsKilled = true
		atomic.StoreInt32(&rf.status, Follower)
		atomic.StoreInt64(&rf.votedFor, -1)
		return
	}
	if args.Term < atomic.LoadInt64(&rf.currentTerm) {
		reply.VoteGranted = false
		reply.IsExpired = true
		reply.Term = atomic.LoadInt64(&rf.currentTerm)
		//atomic.StoreInt64(&rf.votedFor, args.CandidateId)
		return
	}
	if args.Term > atomic.LoadInt64(&rf.currentTerm) {
		atomic.StoreInt32(&rf.status, Follower)
		atomic.StoreInt64(&rf.votedFor, -1)
		atomic.StoreInt64(&rf.currentTerm, args.Term)
		rf.persist()
	}
	if atomic.LoadInt64(&rf.votedFor) == -1 {
		lastLogIndex := int64(len(rf.log))
		lastLogTerm := rf.log.getEntryByIndex(lastLogIndex).Term
		//当Candidate任期大等于本地节点任期，或term相同时Index大等于本地时即可vote
		if args.LastLogTerm > lastLogTerm || (lastLogTerm == args.LastLogTerm && args.LastLogIndex >= lastLogIndex) {
			reply.VoteGranted = true
			atomic.StoreInt64(&rf.votedFor, args.CandidateId)
			rf.LastHeartBeat = time.Now()
			rf.persist()
			DPrintf("%v votedFor %v", rf.me, args.CandidateId)
		} else {
			reply.VoteGranted = false
			reply.IsExpired = true //Candidate的log是过时的
			DPrintf("Candidate%v's log is expired", args.CandidateId)
		}
	} else {
		reply.VoteGranted = false
		rf.LastHeartBeat = time.Now()
	}
}

type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term              int64
	LeaderId          int64
	PrevLogIndex      int64
	PrevLogTerm       int64
	Entries           []Entry
	LeaderCommitIndex int64
}

type AppendEntriesReply struct {
	// Your data here (3A).
	Term      int64
	Success   bool
	IsKilled  bool
	IsExpired bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		reply.IsKilled = true
		reply.Success = false
		atomic.StoreInt64(&rf.votedFor, -1)
		atomic.StoreInt32(&rf.status, Follower)
		return
	}

	if args.Term < atomic.LoadInt64(&rf.currentTerm) {
		reply.IsExpired = true
		reply.Term = atomic.LoadInt64(&rf.currentTerm)
		reply.Success = false
		return
	}
	if rf.log.getEntryByIndex(int64(len(rf.log))).Term > args.Term {
		reply.IsExpired = true
		reply.Success = false
		return
	}
	if len(args.Entries) == 0 { //HeartBeat
		atomic.StoreInt64(&rf.votedFor, args.LeaderId)
		atomic.StoreInt32(&rf.status, Follower)
		rf.LastHeartBeat = time.Now()
		atomic.StoreInt64(&rf.currentTerm, args.Term)
		reply.Success = true
		rf.persist()
	} else {
		rf.LastHeartBeat = time.Now()
		DPrintf("%v get Entry from %v at term %v", rf.me, args.LeaderId, args.Term)
		//如果PrevLogTerm与PrevLogIndex不匹配则nextIndex回退
		if args.PrevLogTerm == -1 || args.PrevLogTerm != rf.log.getEntryByIndex(args.PrevLogIndex).Term {
			reply.Success = false
			return
		}
		exceed := true
		for i, entry := range args.Entries {
			//if len(rf.log)-1 < int(args.PrevLogIndex)+i || rf.log[int(args.PrevLogIndex)+i].Term != entry.Term {
			//	//切片覆盖
			//	rf.log = append(rf.log[0:int(args.PrevLogIndex)+i], args.Entries[i:]...)
			//	break
			//}
			//从这里开始出现不同，Raft是强Leader型算法，因此直接进行覆盖
			if rf.log.getEntryByIndex(int64(i+1)+args.PrevLogIndex).Term != entry.Term {
				rf.log = append(rf.log.getEntrySlice(1, int64(i+1)+args.PrevLogIndex), args.Entries[i:]...)
				exceed = false
				break
			}

		}
		if exceed {
			//删除不同的部分
			rf.log = rf.log.getEntrySlice(1, int64(len(args.Entries))+args.PrevLogIndex)
		}
		reply.Success = true
		rf.persist()
	}
	//如果 leaderCommit>commitIndex，
	//设置本地 commitIndex 为 leaderCommit 和最新日志索引中
	//较小的一个。
	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = args.LeaderCommitIndex
		if int64(len(rf.log)) < rf.commitIndex {
			rf.commitIndex = int64(len(rf.log))
		}
		rf.persist()
	}

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
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	if rf.killed() {
		return index, term, false
	}
	term = int(atomic.LoadInt64(&rf.currentTerm))
	if atomic.LoadInt32(&rf.status) != Leader {
		isLeader = false
	} else {
		rf.mu.Lock()
		logEntry := Entry{
			Command: command,
			Term:    atomic.LoadInt64(&rf.currentTerm),
		}
		rf.log = append(rf.log, logEntry)
		rf.persist()
		DPrintf("Leader%v get new log:%v\n", rf.me, command)

		index = len(rf.log)
		rf.mu.Unlock()
	}

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
	atomic.StoreInt32(&rf.status, Follower)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		timeoutTime := time.Now().Unix()
		ms := 150 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		if rf.LastHeartBeat.Unix() < timeoutTime && atomic.LoadInt32(&rf.status) != Leader {
			go rf.Election() //election will start after mu.Unlock()
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) Election() {
	rf.mu.Lock()
	atomic.StoreInt32(&rf.status, Candidate)
	atomic.AddInt64(&rf.currentTerm, 1)
	atomic.StoreInt64(&rf.votedFor, rf.me)
	rf.persist()
	rf.LastHeartBeat = time.Now()
	var voteCount int64 = 1
	expired := false
	args := &RequestVoteArgs{
		Term:         atomic.LoadInt64(&rf.currentTerm),
		CandidateId:  rf.me,
		LastLogIndex: int64(len(rf.log)),
	}
	if args.LastLogIndex != 0 {
		args.LastLogTerm = rf.log.getEntryByIndex(args.LastLogIndex).Term
	} else {
		args.LastLogTerm = 0
	}
	rf.mu.Unlock()
	currentPeerNum := int64(len(rf.peers))
	//fmt.Println(atomic.LoadInt64(&rf.me), atomic.LoadInt64(&rf.currentTerm))
	for i, _ := range rf.peers {
		if expired {
			break
		}
		if i == int(rf.me) {
			continue
		}
		reply := &RequestVoteReply{}
		//rf.ElectionSync.Add(1)
		go func(serverId int) {
			if rf.sendRequestVote(serverId, args, reply) {
				rf.mu.Lock()
				//DPrintf("%v:%v", rf.me, *reply)
				if reply.IsExpired {
					atomic.StoreInt32(&rf.status, Follower)
					atomic.StoreInt64(&rf.votedFor, -1)
					atomic.StoreInt64(&rf.currentTerm, reply.Term)
					expired = true
					rf.mu.Unlock()
					return
				}
				if reply.IsKilled {
					atomic.AddInt64(&currentPeerNum, -1)
				}
				if reply.VoteGranted && !expired {
					atomic.AddInt64(&voteCount, 1)

					if atomic.LoadInt64(&voteCount) > int64(len(rf.peers))/2 /*atomic.LoadInt64(&currentPeerNum)/2*/ && atomic.LoadInt32(&rf.status) == Candidate {
						atomic.StoreInt32(&rf.status, Leader)
						rf.persist()
					}
				}
				rf.mu.Unlock()
			} else {
				atomic.AddInt64(&currentPeerNum, -1)
			}
			//rf.ElectionSync.Done()
		}(i)
	}
	time.Sleep(20 * time.Millisecond)
	//rf.ElectionSync.Wait()
	if expired {
		atomic.StoreInt32(&rf.status, Follower)
		rf.persist()
	}
	if atomic.LoadInt32(&rf.status) == Leader {
		DPrintf("%v become Leader in term %v", rf.me, rf.currentTerm)
		rf.DiscoverServers()
	}
}

func (rf *Raft) DiscoverServers() {
	for atomic.LoadInt32(&rf.status) == Leader && !rf.killed() {
		//rf.mu.Lock()
		rf.LastHeartBeat = time.Now()
		//DPrintf("%v %v", rf.currentTerm, rf.me)

		//rf.mu.Unlock()
		lastLogIndex := int64(len(rf.log))
		for i, _ := range rf.peers {
			if atomic.LoadInt32(&rf.status) != Leader {
				break
			}
			if i == int(rf.me) {
				continue
			}
			args := &AppendEntriesArgs{
				Term:              atomic.LoadInt64(&rf.currentTerm),
				LeaderId:          rf.me,
				LeaderCommitIndex: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}
			//lab2B
			prevLogIndex := rf.NextIndex[i] - 1
			if lastLogIndex == 0 {
				args.PrevLogIndex = 0
				args.PrevLogTerm = 0
			} else {
				args.PrevLogIndex = prevLogIndex
				args.PrevLogTerm = rf.log.getEntryByIndex(prevLogIndex).Term
			}
			if lastLogIndex > args.PrevLogIndex {
				//DPrintf("Leader%v start AppendEntries to %v,lastLogIndex:%v PrevLogIndex:%v", rf.me, i, lastLogIndex, args.PrevLogIndex)
				entries := rf.log.getEntrySlice(prevLogIndex+1, lastLogIndex+1)
				args.Entries = entries
				go rf.sendEntries(args, i) //由于涉及到多次调用，封装到函数中
			} else {
				//lab2A
				go func(serverId int) {
					if rf.sendAppendEntries(serverId, args, reply) {
						if reply.IsExpired {
							atomic.StoreInt64(&rf.currentTerm, reply.Term)
							atomic.StoreInt32(&rf.status, Follower)
							atomic.StoreInt64(&rf.votedFor, -1)
							rf.persist()
							return
						}
					}

				}(i)

			}

		}
		ms := 201
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}

}

func (rf *Raft) sendEntries(args *AppendEntriesArgs, serverId int) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	reply := &AppendEntriesReply{}
	if rf.sendAppendEntries(serverId, args, reply) {
		if reply.IsKilled {
			return
		}
		if reply.IsExpired {
			atomic.StoreInt64(&rf.currentTerm, reply.Term)
			atomic.StoreInt32(&rf.status, Follower)
			atomic.StoreInt64(&rf.votedFor, -1)
			return
		}
		//3.1 如果成功，更新nextIndex和matchIndex
		if reply.Success {
			rf.mu.Lock()
			rf.NextIndex[serverId] = int64(len(rf.log)) + 1
			//rf.nextIndex[serverId] += int64(len(args.Entries))
			//rf.matchIndex[serverId] = int64(len(rf.log))
			rf.MatchIndex[serverId] = args.PrevLogIndex + int64(len(args.Entries))
			rf.persist()
			for N := int64(len(rf.log)); N > atomic.LoadInt64(&rf.commitIndex) && rf.log.getEntryByIndex(N).Term == atomic.LoadInt64(&rf.currentTerm); N-- {
				count := 1
				for s, matchIndex := range rf.MatchIndex {
					if s == int(rf.me) {
						continue
					}
					if matchIndex >= N {
						count++
					}
				}
				//4.majority(matchIndex[i]>= N)（如果参与者大多数的最新日志的索引大于 N）
				if count > len(rf.peers)/2 {
					rf.commitIndex = N
					rf.persist()
					break
				}
			}
			rf.mu.Unlock()
		} else {
			//3.2 如果由于日志不一致而失败，减少 nextIndex 并重试
			if rf.NextIndex[serverId] > 1 {
				rf.NextIndex[serverId]--
				rf.persist()
			}

			lastLogIndex := int64(len(rf.log))
			args = &AppendEntriesArgs{
				Term:              atomic.LoadInt64(&rf.currentTerm),
				LeaderId:          rf.me,
				LeaderCommitIndex: rf.commitIndex,
			}
			reply = &AppendEntriesReply{}
			//lab2B
			prevLogIndex := rf.NextIndex[serverId] - 1
			if lastLogIndex == 0 {
				args.PrevLogIndex = 0
				args.PrevLogTerm = 0
			} else {
				args.PrevLogIndex = prevLogIndex
				args.PrevLogTerm = rf.log.getEntryByIndex(prevLogIndex).Term
			}
			//DPrintf("out-sync:Leader%v restart AppendEntries to %v", rf.me, serverId)
			entries := rf.log.getEntrySlice(prevLogIndex+1, lastLogIndex+1)
			args.Entries = entries
			go rf.sendEntries(args, serverId) //由于涉及到多次调用，封装到函数中
		}
	} else {
		//fmt.Println(serverId, "is disconn")
	}
}

func (rf *Raft) applyLogToStateMachine(applyCh chan ApplyMsg) {
	for !rf.killed() {
		appliedMsgs := []ApplyMsg{}

		for atomic.LoadInt64(&rf.commitIndex) > atomic.LoadInt64(&rf.lastApplied) {
			DPrintf("Raft%v applyLogToStateMachine,commitIndex=%v,lastApplied=%v\n", rf.me, rf.commitIndex, rf.lastApplied)
			rf.lastApplied++
			appliedMsgs = append(appliedMsgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.log.getEntryByIndex(rf.lastApplied).Command,
				CommandIndex: int(rf.lastApplied),
			})

			for _, msg := range appliedMsgs {
				applyCh <- msg
			}
			rf.persist()
		}

		time.Sleep(50 * time.Millisecond)
	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = int64(me)

	// Your initialization code here (3A, 3B, 3C).
	rf.status = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.LastHeartBeat = time.Now()
	//rf.ElectionSync = sync.WaitGroup{}
	//rf.AppendEntriesSync = sync.WaitGroup{}
	rf.log = make([]Entry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.NextIndex = make([]int64, len(rf.peers))
	for i, _ := range rf.NextIndex {
		rf.NextIndex[i] = 1
	}
	rf.MatchIndex = make([]int64, len(rf.peers))
	for i, _ := range rf.MatchIndex {
		rf.MatchIndex[i] = 0
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLogToStateMachine(applyCh)
	return rf
}
