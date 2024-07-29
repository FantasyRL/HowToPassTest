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
	"github.com/sasha-s/go-deadlock"
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
	ElectionSync  sync.WaitGroup
	//3B
	log         []Entry
	commitIndex int64
	lastApplied int64
	nextIndex   []int64
	matchIndex  []int64
}

//type ServerStatus int32

const (
	Follower = iota
	Candidate
	Leader
)

type Entry struct {
	Term    int64
	Command interface{} //machine command
	Index   int64
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
	if rf.killed() {
		reply.VoteGranted = false
		reply.IsExpired = false
		reply.IsKilled = true
		atomic.StoreInt32(&rf.status, Follower)
		atomic.StoreInt64(&rf.votedFor, -1)
		rf.mu.Unlock()
		return
	}
	if args.Term < atomic.LoadInt64(&rf.currentTerm) {
		reply.VoteGranted = false
		reply.IsExpired = true
		reply.Term = atomic.LoadInt64(&rf.currentTerm)
		atomic.StoreInt64(&rf.votedFor, args.CandidateId)
		rf.mu.Unlock()
		return
	}
	if args.Term > atomic.LoadInt64(&rf.currentTerm) {
		atomic.StoreInt32(&rf.status, Follower)
		atomic.StoreInt64(&rf.votedFor, -1)
		atomic.StoreInt64(&rf.currentTerm, args.Term)
	}
	if atomic.LoadInt64(&rf.votedFor) == -1 {
		//currentLogIndex := 0
		//if len(rf.log)-1 >= 0 {
		//	currentLogIndex = rf.log[currentLogIndex].Term
		//}
		//if len(rf.log)-1 <= args.LastLogIndex && args.LastLogIndex >= currentLogIndex {
		reply.VoteGranted = true
		atomic.StoreInt64(&rf.votedFor, args.CandidateId)
		rf.LastHeartBeat = time.Now()
		//} else {
		//reply.VoteGranted = false
		//reply.IsExpired = true
		//}
	} else {
		reply.VoteGranted = false
		rf.LastHeartBeat = time.Now()
	}
	rf.mu.Unlock()
}

type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term         int64
	LeaderId     int64
	PrevLogIndex int64
	PrevLogTerm  int64
	//entries[]
	LeaderCommitIndex int64
}

type AppendEntriesReply struct {
	// Your data here (3A).
	Term    int64
	Success bool

	IsExpired bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
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
	atomic.StoreInt64(&rf.votedFor, args.LeaderId)
	rf.LastHeartBeat = time.Now()
	atomic.StoreInt64(&rf.currentTerm, args.Term)

	reply.Success = true
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
		ms := 50 + (rand.Int63() % 300)
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
	rf.LastHeartBeat = time.Now()
	var voteCount int64 = 1
	expired := false
	args := &RequestVoteArgs{
		Term:         atomic.LoadInt64(&rf.currentTerm),
		CandidateId:  rf.me,
		LastLogIndex: int64(len(rf.log) - 1),
	}
	if args.LastLogIndex != -1 {
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
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
		rf.ElectionSync.Add(1)
		go func(serverId int) {
			defer rf.ElectionSync.Done()
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

					if atomic.LoadInt64(&voteCount) > atomic.LoadInt64(&currentPeerNum)/2 && atomic.LoadInt32(&rf.status) == Candidate {
						atomic.StoreInt32(&rf.status, Leader)
					}
				}
				rf.mu.Unlock()
			} else {
				atomic.AddInt64(&currentPeerNum, -1)
			}

		}(i)
	}
	time.Sleep(20 * time.Millisecond)
	rf.ElectionSync.Wait()
	if expired {
		atomic.StoreInt32(&rf.status, Follower)
	}
	if atomic.LoadInt32(&rf.status) == Leader {
		rf.DiscoverServers()
	}
}

func (rf *Raft) DiscoverServers() {
	for atomic.LoadInt32(&rf.status) == Leader && !rf.killed() {
		rf.mu.Lock()
		rf.LastHeartBeat = time.Now()
		//DPrintf("%v %v", rf.currentTerm, rf.me)
		args := &AppendEntriesArgs{
			Term:              atomic.LoadInt64(&rf.currentTerm),
			LeaderId:          rf.me,
			LeaderCommitIndex: rf.commitIndex,
		}
		rf.mu.Unlock()
		for i, _ := range rf.peers {
			if atomic.LoadInt32(&rf.status) != Leader {
				break
			}
			if i == int(rf.me) {
				continue
			}
			reply := &AppendEntriesReply{}
			//rf.ElectionSync.Add(1)
			go func(serverId int) {
				//defer rf.ElectionSync.Done()
				if rf.sendAppendEntries(serverId, args, reply) {
					if reply.IsExpired {
						atomic.StoreInt64(&rf.currentTerm, reply.Term)
						atomic.StoreInt32(&rf.status, Follower)
						atomic.StoreInt64(&rf.votedFor, -1)
						return
					}
				}

			}(i)
		}
		ms := 101
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf.nextIndex = make([]int64, 0)
	rf.nextIndex = make([]int64, 0)
	rf.matchIndex = make([]int64, 0)
	rf.log = make([]Entry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.LastHeartBeat = time.Now()
	rf.ElectionSync = sync.WaitGroup{}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
