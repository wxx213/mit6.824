package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	OpPut            = "PUT"
	OpAppend         = "APPEND"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	TraceId int
	RequestId int
}

type PutAppendReply struct {
	Err Err
	Index int
	Term int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	TraceId int
}

type GetReply struct {
	Err   Err
	Value string
}