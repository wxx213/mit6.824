package kvraft

import (
	"com.example.mit6_824/src/labgob"
	"com.example.mit6_824/src/labrpc"
	"log"
	"com.example.mit6_824/src/raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func init() {
	if Debug > 0 {
		log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
	}
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	KEY   string
	VALUE string
	OP    string // "Put" or "Append"
	APPLYID int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mapKv 	map[string]string
	applyId []int
	applyIndex int
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//DPrintf("server %d received Get request %+v", kv.me, args)
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	value, ok := kv.mapKv[args.Key]
	if ok {
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	kv.mu.Unlock()
	DPrintf("leader %d reveived Get request %+v, reply: %+v, traceid: %d ", kv.me, args, reply, args.TraceId)
}

func findApplyId(applyids []int, id int) bool {
	for _,applyId := range applyids {
		if applyId == id {
			return true
		}
	}
	return false
}

func waitLogApply(kv *KVServer, args *PutAppendArgs, reply *PutAppendReply, index int, term int) {
	doneCh := make(chan bool)
	go func() {
		for  {
			kv.mu.Lock()
			if kv.applyIndex >= index {
				if findApplyId(kv.applyId, args.ApplyId) {
					reply.Err = OK
				} else {
					reply.Err = ErrWrongLeader
				}
				close(doneCh)
				kv.mu.Unlock()
				break
			}
			kv.mu.Unlock()
			time.Sleep(raft.Logapplyperiod)
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				reply.Err = ErrWrongLeader
				reply.Index = index
				reply.Term = term
				close(doneCh)
				break
			}
		}
	}()
	<- doneCh
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//DPrintf("server %d received PutAppend request %+v", kv.me, args)

	_, leader := kv.rf.GetState()
	if !leader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	exist := findApplyId(kv.applyId, args.ApplyId)
	kv.mu.Unlock()
	if exist{
		//DPrintf("leader %d already reveived PutAppend request %+v, traceid: %d", kv.me, args, args.TraceId)
		reply.Err = OK
		return
	}

	// the request may exist in some servers.
	// if the request exist in current leader, wait for commit.
	// if not, continue start new request
	if args.Index != -1 && args.Term != -1 &&
		kv.rf.CheckLogExist(args.Index, args.Term) {
		waitLogApply(kv, args, reply, args.Index, args.Term)
		return
	}

	kvOp := Op{
		KEY: args.Key,
		VALUE: args.Value,
		OP: args.Op,
		APPLYID: args.ApplyId,
	}
	index,term,isLeader := kv.rf.Start(kvOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		DPrintf("leader %d reveived PutAppend request %+v, index: %d, term: %d, traceid: %d ", kv.me, args, index, term, args.TraceId)
		waitLogApply(kv, args, reply, index, term)
		// DPrintf("leader %d resolved PutAppend request %+v, index: %d, term: %d, traceid: %d ", kv.me, args, index, term, args.TraceId)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.mapKv = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	// go routine to operate the kv from applied log
	go func() {
		for m := range kv.applyCh {
			if m.CommandValid {
				DPrintf("server %d received log apply %+v", kv.me, m)
				kv.mu.Lock()
				op, ok := m.Command.(Op)
				if ok {
					if op.OP == OpPut {
						kv.mapKv[op.KEY] = op.VALUE
					} else if op.OP == OpAppend {
						value,ok := kv.mapKv[op.KEY]
						if ok {
							kv.mapKv[op.KEY] = value + op.VALUE
						} else {
							kv.mapKv[op.KEY] = op.VALUE
						}
					}
					kv.applyId = append(kv.applyId, op.APPLYID)
				}
				kv.applyIndex = m.CommandIndex
				kv.mu.Unlock()
			}
		}
	}()
	return kv
}
