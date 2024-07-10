package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type BaseMsg struct {
	Code int //200 Success 400NoWork
	Msg  string
}

type RPCArgs struct {
	Status int //0接任务 1完成
	Stage  int //1Map 2Reduce
	//0 param
	WorkerNum int
	//1 param
	WorkSerial int
	NSerial    int
	//Intermediate *[]KeyValue
}

type RPCReply struct {
	FileName     string
	WorkSerial   int
	BaseMsg      *BaseMsg //200Map 300Reduce 400Done 500wait
	NReduceCount int
	NSerial      int //nReduce serial
	WorkCount    int
	//Intermediate *[]KeyValue
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func NewBaseMsg(code int, Msg string) *BaseMsg {
	return &BaseMsg{Code: code, Msg: Msg}
}
