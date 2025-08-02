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
