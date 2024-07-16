package kvsrv

import (
	"log"
	"strings"
	"sync"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Queue interface {
	Push(int)
	Pop()
	GetTop() int
}

func (kv *KVServer) Push(i int) {
	kv.queue = append(kv.queue, i)
}
func (kv *KVServer) Pop() {
	kv.queue = kv.queue[1:]
}
func (kv *KVServer) GetPop() int {
	return kv.queue[0]
}

type KVServer struct {
	mu                 sync.Mutex
	kva                map[string]string
	isWorking          chan bool
	RecentPutForAppend bool
	queue              []int
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("%s", "get call")
	// Your code here.
	kv.mu.Lock()
	kv.Push(1)
	for {
		if kv.GetPop() == 2 {
			time.Sleep(time.Millisecond * 10)
		} else {
			break
		}
	}

	if value, ok := kv.kva[args.Key]; ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
	//DPrintf("Get:%v\n", kv.kva)

	kv.Pop()
	kv.mu.Unlock()
	reply.MsgCode = 200

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("%s", "put call")
	// Your code here.
	//if <-kv.isWorking {
	kv.mu.Lock()
	kv.Push(2)

	kv.kva[args.Key] = args.Value
	//DPrintf("Get:%v\n", kv.kva)

	kv.Pop()
	kv.mu.Unlock()
	reply.MsgCode = 200
	//}
	//kv.isWorking <- true
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("%s", "append call")
	//if <-kv.isWorking {
	// Your code here.
	kv.mu.Lock()
	kv.Push(3)
	for {
		if kv.GetPop() == 2 {
			time.Sleep(time.Millisecond * 10)
		} else {
			break
		}
	}

	if value, ok := kv.kva[args.Key]; ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
	kv.kva[args.Key] = strings.Join([]string{kv.kva[args.Key], args.Value}, "")

	kv.Pop()
	kv.mu.Unlock()
	reply.MsgCode = 200
	//}

	//kv.isWorking <- true
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kva = make(map[string]string)
	kv.isWorking = make(chan bool, 1)
	kv.queue = make([]int, 0)
	kv.isWorking <- true
	kv.RecentPutForAppend = false
	// You may need initialization code here.

	return kv
}
