package kvraft

import (
	"com.example.mit6_824/src/labrpc"
	"com.example.mit6_824/src/raft"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	arg := GetArgs{
		Key: key,
	}
	reply := GetReply{}
repeat:
	for i:=0;i<len(ck.servers);i++ {
		ok := ck.servers[i].Call("KVServer.Get", &arg, &reply)
		if !ok {
			continue
		} else if reply.Err == OK {
			return reply.Value
		}
	}
	if reply.Err == ErrWrongLeader {
		time.Sleep(raft.Electiontimeoutbase * time.Millisecond)
		goto repeat
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	applyId := (int)(nrand())
	arg := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		ApplyId: applyId,
	}
	reply := PutAppendReply{}
repeat:
	for i:=0;i<len(ck.servers);i++ {
		traceId := nrand()
		arg.TraceId = (int)(traceId);
		ok := ck.servers[i].Call("KVServer.PutAppend", &arg, &reply)
		if !ok {
			DPrintf("traceid: %d client send PutAppend request %+v net error", arg.TraceId, arg)
			continue
		} else if reply.Err == OK {
			DPrintf("traceid: %d client sended PutAppend request %+v",arg.TraceId,  arg)
			return
		} else {
			DPrintf("traceid: %d client send PutAppend request %+v error: %+v", arg.TraceId, arg, reply.Err)
		}
	}
	if reply.Err == ErrWrongLeader {
		time.Sleep(raft.Electiontimeoutbase * time.Millisecond)
		goto repeat
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
