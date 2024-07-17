package kvsrv

import (
	"log"
	"strings"
	"sync"
	"time"
)

const Debug = false

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
func (kv *KVServer) GetTop() int {
	return kv.queue[0]
}

type KVServer struct {
	mu  sync.Mutex
	kva map[string]string
	//isWorking          chan bool
	RecentPutForAppend bool
	queue              []int
	recoverReply       map[int]string //todo:optimize
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//DPrintf("%s", "get call")
	// Your code here.
	kv.mu.Lock()
	if args.IsNoDie {
		delete(kv.recoverReply, args.WorkerId)
		kv.mu.Unlock()
		return
	}
	if value, ok := kv.recoverReply[args.WorkerId]; ok && args.IsRecover {
		//if value != "nil" {
		reply.Value = value
		//delete(kv.recoverReply, args.WorkerId)
		kv.mu.Unlock()
		return
		//}
		//delete(kv.recoverReply, args.WorkerId)
	}

	//if !args.IsRecover {
	kv.Push(args.WorkerId)
	//}
	for {
		//DPrintf("bao")
		if kv.GetTop() != args.WorkerId {
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

	kv.recoverReply[args.WorkerId] = reply.Value
	kv.Pop()
	kv.mu.Unlock()
	reply.MsgCode = 200

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	//DPrintf("%s", "put call")
	// Your code here.
	//if <-kv.isWorking {
	kv.mu.Lock()
	if args.IsNoDie {
		delete(kv.recoverReply, args.WorkerId)
		kv.mu.Unlock()
		return
	}
	if value, ok := kv.recoverReply[args.WorkerId]; ok && args.IsRecover {
		//if value != nil {
		reply.Value = value
		//delete(kv.recoverReply, args.WorkerId)
		kv.mu.Unlock()
		return
		//}
		//delete(kv.recoverReply, args.WorkerId)
	}
	//if !args.IsRecover {
	kv.Push(args.WorkerId)
	//}
	for {
		//DPrintf("bao")
		if kv.GetTop() != args.WorkerId {
			time.Sleep(time.Millisecond * 10)
		} else {
			break
		}
	}

	kv.kva[args.Key] = args.Value
	//DPrintf("Get:%v\n", kv.kva)

	kv.recoverReply[args.WorkerId] = args.Value
	kv.Pop()
	kv.mu.Unlock()
	reply.MsgCode = 200
	//}
	//kv.isWorking <- true
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	//DPrintf("%s", "append call")
	//if <-kv.isWorking {
	// Your code here.
	kv.mu.Lock()
	if args.IsNoDie {
		delete(kv.recoverReply, args.WorkerId)
		kv.mu.Unlock()
		return
	}
	if value, ok := kv.recoverReply[args.WorkerId]; ok && args.IsRecover {
		//if value != nil {
		reply.Value = value
		//delete(kv.recoverReply, args.WorkerId)
		kv.mu.Unlock()
		return
		//}
		//delete(kv.recoverReply, args.WorkerId)
		//kv.recoverReply[args.WorkerId] = nil
	}
	//if !args.IsRecover {
	kv.Push(args.WorkerId)
	//}
	for {
		//DPrintf("bao")
		if kv.GetTop() != args.WorkerId {
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

	kv.recoverReply[args.WorkerId] = reply.Value
	kv.Pop()
	kv.mu.Unlock()
	reply.MsgCode = 200
	//}

	//kv.isWorking <- true
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kva = make(map[string]string)
	kv.recoverReply = make(map[int]string)
	//kv.isWorking = make(chan bool, 1)
	kv.queue = make([]int, 0)
	//kv.isWorking <- true
	kv.RecentPutForAppend = false
	// You may need initialization code here.

	return kv
}
