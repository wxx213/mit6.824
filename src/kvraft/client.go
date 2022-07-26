package kvraft

import (
	"com.example.mit6_824/src/labrpc"
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
	requestId := (int)(nrand())
	arg := GetArgs{
		Key: key,
		RequestId: requestId,
	}
	reply := GetReply{}

	var server, i int
	for  {
		traceId := nrand()
		arg.TraceId = (int)(traceId)
		server = i % len(ck.servers)
		i++
		ok := ck.servers[server].Call("KVServer.Get", &arg, &reply)
		if !ok {
			continue
		} else if reply.Err == OK {
			DPrintf("client sended Get request %+v, value: %s traceid: %d", arg, reply.Value, arg.TraceId)
			return reply.Value
		}
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
	requestId := (int)(nrand())
	arg := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		RequestId: requestId,
	}
	reply := PutAppendReply{}

	var server, i int
	for {
		traceId := nrand()
		arg.TraceId = (int)(traceId)
		server = i % len(ck.servers)
		i++
		ok := ck.servers[server].Call("KVServer.PutAppend", &arg, &reply)
		if !ok {
			DPrintf("client send PutAppend request %+v net error, traceid: %d", arg, arg.TraceId)
			continue
		} else if reply.Err == OK {
			DPrintf("client sended PutAppend request %+v, traceid: %d", arg, arg.TraceId)
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
