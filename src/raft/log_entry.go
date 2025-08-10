package raft

//type EntryCmd interface {
//	Len() int
//	GetLast() *Entry
//	Find(index int) *Entry
//	Split(n int) (firstN LogEntries, remaining LogEntries)
//}

type Entry struct {
	Term    int64
	Command interface{} //machine command
	//Index int64
}
type LogEntries []Entry

func (l *LogEntries) Len() int {
	return len(*l)
}
func (l *LogEntries) GetLast() *Entry {
	if l.Len() == 0 {
		return nil
	}
	return &(*l)[l.Len()-1]
}

// Find 根据索引查找日志条目
func (l *LogEntries) Find(index int64) *Entry {
	lCopy := *l
	if int64(lCopy.Len()) > index {
		return &(*l)[index]
	}
	return nil
}

// Split 把日志分割成前n个（包括l[0]）和从n开始的两部分
func (l *LogEntries) Split(n int64) (firstN LogEntries, remaining LogEntries) {
	lCopy := *l
	if n <= 0 || lCopy.Len() == 0 {
		return nil, lCopy
	}
	if n >= int64(lCopy.Len()) {
		return lCopy, nil
	}
	return lCopy[:n], lCopy[n:]
}

func (l *LogEntries) Append(entry LogEntries) {
	if entry != nil {
		*l = append(*l, entry...)
	}
}

// lab3D
// toSliceIndex 将日志条目的索引转换为当前切片下标
func (rf *Raft) toSliceIndex(index int64) (int, bool) {
	// index == lastIncludedIndex：允许访问“哨兵条目”，它在切片下标 0
	if index < rf.lastIncludedIndex {
		return -1, false // 已被快照丢弃
	}
	// 当前切片第 0 位代表 lastIncludedIndex
	off := index - rf.lastIncludedIndex
	if off < 0 {
		return -1, false
	}
	if off >= int64(len(rf.logEntries)) {
		return -1, false
	}
	return int(off), true
}

// termAt3D 读取 index 处的任期（考虑快照偏移）
func (rf *Raft) termAt3D(index int64) (term int64, ok bool) {
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm, true
	}
	i, ok := rf.toSliceIndex(index)
	if !ok {
		return 0, false
	}
	return rf.logEntries[i].Term, true
}

// Find3D 读取 index 处的 Entry 指针
func (rf *Raft) Find3D(index int64) *Entry {
	if index == rf.lastIncludedIndex {
		// 同样存在一个哨兵日志
		return &Entry{Term: rf.lastIncludedTerm, Command: nil}
	}
	i, ok := rf.toSliceIndex(index)
	if !ok {
		return nil
	}
	return &rf.logEntries[i]
}

// SplitEntriesFrom3D 返回从 from 开始的日志条目切片
func (rf *Raft) SplitEntriesFrom3D(from int64) LogEntries {
	if from <= rf.lastIncludedIndex {
		// from 落在快照里：严格来说该发 InstallSnapshot（3D）
		// 先返回从切片起点开始（跳过哨兵）的所有日志，3D 时替换为 snapshot 逻辑
		if len(rf.logEntries) <= 1 {
			return nil
		}
		out := make(LogEntries, len(rf.logEntries)-1)
		copy(out, rf.logEntries[1:])
		return out
	}
	i, ok := rf.toSliceIndex(from)
	if !ok {
		return nil
	}
	out := make(LogEntries, len(rf.logEntries)-i)
	copy(out, rf.logEntries[i:])
	return out
}

// ReservePrefix3D 截断日志条目，保留到 index（含）
func (rf *Raft) ReservePrefix3D(index int64) {
	// 允许 index == lastIncludedIndex（只保留哨兵）
	if index < rf.lastIncludedIndex {
		// 都在快照里，无需保留实际日志
		rf.logEntries = rf.logEntries[:1] // 只留哨兵日志
		rf.lastLogIndex.Store(rf.lastIncludedIndex)
		return
	}
	i, ok := rf.toSliceIndex(index)
	if !ok {
		// index > last：无需截断
		return
	}
	rf.logEntries = rf.logEntries[:i+1] // 保留到 index（含）
	rf.lastLogIndex.Store(index)
}
