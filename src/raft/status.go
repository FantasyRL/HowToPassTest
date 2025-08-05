package raft

import (
	"sync/atomic"
	"time"
)

var (
	FollowerStatus  = int32(0)
	CandidateStatus = int32(1)
	LeaderStatus    = int32(2)
)

type WorkerStatus struct {
	value atomic.Int32
}

func (s *WorkerStatus) Set(status int32) {
	s.value.Store(status)
}

func NewWorkerStatusFromInt(status int32) *WorkerStatus {
	s := &WorkerStatus{}
	s.Set(status)
	return s
}

func (s *WorkerStatus) Equal(status int32) bool {
	return s.value.Load() == status
}

func (rf *Raft) becomeFollower(term int64) {
	rf.workerStatus.Set(FollowerStatus)
	rf.currentTerm.Store(term)
	rf.votedFor.Store(-1)
	rf.resetElectionTimer()
	// 任期变化，persist
	rf.persist()
}

func (rf *Raft) becomeCandidate() {
	rf.workerStatus.Set(CandidateStatus)
	rf.currentTerm.Add(1)
	rf.votedFor.Store(int64(rf.me)) // 先票给自己
	rf.resetElectionTimer()
	// 任期与votedFor变化，persist
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	if !rf.workerStatus.Equal(LeaderStatus) {
		// leader的可变状态（每次选举后重新初始化）
		for i := range rf.peers {
			rf.nextIndex[i] = int(rf.lastLogIndex.Load() + 1)
			rf.matchIndex[i] = 0 // 表示还没有匹配的日志
		}
		DPrintf("[LEADER_ELECTIONED] %d become leader, term %d", rf.me, rf.currentTerm.Load())
		rf.workerStatus.Set(LeaderStatus)
		go func() {
			for {
				if rf.killed() || !rf.workerStatus.Equal(LeaderStatus) {
					return
				}
				rf.sendHeartbeatsAndLogEntries()
				time.Sleep(50 * time.Millisecond) // 50ms < electionTimeoutMin(300ms)
			}
		}()

	}
	rf.resetElectionTimer() // 防止leader心跳过期
}
