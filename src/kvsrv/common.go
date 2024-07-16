package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	WorkerId  int
	IsRecover bool
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	MsgCode int //200success 500error
	Value   string
}

type GetArgs struct {
	Key       string
	Value     string
	WorkerId  int
	IsRecover bool
	// You'll have to add definitions here.
}

type GetReply struct {
	MsgCode int //200success 500error
	Value   string
}
