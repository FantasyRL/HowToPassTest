package kvsrv

import (
	"6.5840/labrpc"
	rand2 "math/rand"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	server *labrpc.ClientEnd
	mu     sync.Mutex
	//todo: You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server

	// You'll have to add code here.

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)

// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	//You will have to modify this function.
	args := &GetArgs{
		Key:      key,
		WorkerId: rand2.Int(),
	}
	reply := &GetReply{}
	ck.mu.Lock()
	ok := false
	for {
		ok = ck.server.Call("KVServer.Get", args, reply)
		if ok {
			ck.mu.Unlock()
			return reply.Value
		} else {
			//DPrintf("get rpc recovering")
			args.IsRecover = true
			continue
		}
	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		WorkerId: rand2.Int(),
	}
	reply := &PutAppendReply{}
	ck.mu.Lock()
	ok := false
	for {
		ok = ck.server.Call("KVServer."+op, args, reply)
		if ok {
			ck.mu.Unlock()
			return reply.Value //just for append
		} else {
			//DPrintf(op + "rpc call recovering")
			args.IsRecover = true
			continue
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	//DPrintf("Put %s %s", key, value)
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
