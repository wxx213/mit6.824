package kvraft

import (
	"com.example.mit6_824/src/labgob"
	"com.example.mit6_824/src/labrpc"
	"errors"
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
	REQUESTID int
}

type RequestRes struct {
	index int
	term int
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
	requestIds []int
	lastApplyIndex int
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
	exist := findRequestIdUnlocked(kv, args.RequestId)
	kv.mu.Unlock()
	if !exist {
		kvOp := Op {
			OP: OpGet,
			REQUESTID: args.RequestId,
		}
		_,_,isLeader := kv.rf.Start(kvOp)
		if !isLeader {
			reply.Err = ErrWrongLeader
		} else {
			err := waitLogApply(kv, args.RequestId)
			if err != nil {
				reply.Err = ErrWrongLeader
				return
			}
		}
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

// Wait the log applied in current node.
func waitLogApply(kv *KVServer, requestId int) error {
	doneCh := make(chan error)
	go func() {
		before := time.Now()
		for  {
			now := time.Now()
			if (now.Second() - before.Second()) > 10  {
				doneCh <- errors.New("time out")
				break
			}
			kv.mu.Lock()
			exist := findRequestIdUnlocked(kv, requestId)
			kv.mu.Unlock()

			if exist {
				doneCh <- nil
				break
			}
			time.Sleep(raft.Logapplyperiod)
			_, leader := kv.rf.GetState()
			if !leader {
				doneCh <- errors.New("leader changed")
				break
			}
		}
		close(doneCh)
	}()
	return <- doneCh
}

func findRequestIdUnlocked(kv *KVServer, id int) bool {
	for _,request := range kv.requestIds {
		if request == id {
			return true
		}
	}
	return false
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
	exist := findRequestIdUnlocked(kv, args.RequestId)
	kv.mu.Unlock()
	if exist {
		reply.Err = OK
		return
	}

	kvOp := Op {
		KEY: args.Key,
		VALUE: args.Value,
		OP: args.Op,
		REQUESTID: args.RequestId,
	}
	index,term,isLeader := kv.rf.Start(kvOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		DPrintf("leader %d reveived PutAppend request %+v, index: %d, term: %d, traceid: %d ",
			kv.me, args, index, term, args.TraceId)
		err := waitLogApply(kv, args.RequestId)
		if err != nil {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
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
				kv.mu.Lock()
				op, ok := m.Command.(Op)
				if ok {
					if !findRequestIdUnlocked(kv, op.REQUESTID) {
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
						kv.requestIds = append(kv.requestIds, op.REQUESTID)
						DPrintf("server %d received log apply %+v", kv.me, m)
					}
				}
				kv.lastApplyIndex = m.CommandIndex
				kv.mu.Unlock()
			}
		}
	}()
	return kv
}
