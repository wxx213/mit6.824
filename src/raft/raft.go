package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"com.example.mit6_824/src/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

const (
	Follwer = 0
	Candadtite = 1
	Leader = 2
	Electiontimeout = 450// time.Millisecond
	Hearteatperiod = 200 *time.Millisecond
	RPCTtimeout = 100 * time.Millisecond
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state for all servers
	currentTerm int
	votedFor int

	// volatile state for all servers
	roleState int
	commitIndex int
	lastApplied int

	// volatile state for leader


	// extra.
	// for followers
	heartbeatReceived bool
	// for debug
	electiontimeoutms int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.roleState == Leader
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	TERM int
	CANDIDATEID int
	LASTLOGINDEX int
	LASTLOGTERM int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	TERM int
	VOTEGRANTED bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.TERM >= rf.currentTerm {
		rf.votedFor = args.CANDIDATEID
		rf.currentTerm = args.TERM
		rf.roleState = Follwer
		reply.VOTEGRANTED = true
	} else {
		reply.VOTEGRANTED = false
	}
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


type AppendEntriesArgs struct {
	TERM int
	LEADERID int
	PREVLOGINDEX int
	PREVLOGTERM int
	ENTRIES []byte
	LEADERCOMMIT int
}

type AppendEntriesReply struct {
	TERM int
	SUCCESS bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.TERM >= rf.currentTerm {
		rf.heartbeatReceived = true
		rf.currentTerm = args.TERM
		rf.roleState = Follwer
		reply.SUCCESS = true
	} else {
		reply.SUCCESS = false
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func sendRequestVoteRPC(rf *Raft, server int, grantNum *int32, ch chan bool) {
	args := &RequestVoteArgs{
		TERM: rf.currentTerm,
		CANDIDATEID: rf.me,
	}
	reply := &RequestVoteReply{
		VOTEGRANTED: false,
	}
	doneCh := make(chan bool)
	go func() {
		ok := rf.sendRequestVote(server, args, reply)
		doneCh <- ok
	}()
	select {
	case <-doneCh:
		if reply.VOTEGRANTED == true {
			atomic.AddInt32(grantNum, 1)
		}
	case <-time.After(RPCTtimeout):
	}
	close(ch)
}

func sendAppendEntryRPC(rf *Raft, server int, failedNum *int32, ch chan bool) {
	args := &AppendEntriesArgs{
		TERM: rf.currentTerm,
		LEADERID: rf.me,
	}
	reply := &AppendEntriesReply{
		SUCCESS: false,
	}
	doneCh := make(chan bool)
	go func() {
		ok := rf.sendAppendEntries(server, args, reply)
		doneCh <- ok
	}()
	select {
	case <-doneCh:
		if reply.SUCCESS == false {
			DPrintf("leader %d lost sync node %d", rf.me, server)
			atomic.AddInt32(failedNum, 1)
		}
	case <-time.After(RPCTtimeout):
	}
	close(ch)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.roleState = Follwer
	rf.heartbeatReceived = false
	rf.votedFor = -1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// goroutine for leader election
	timeOut := Electiontimeout + (rand.Int63() % 100)
	rf.electiontimeoutms = int(timeOut)
	DPrintf("node %d init election timeoutms: %d", rf.me, timeOut)
	go func() {
		for {
			time.Sleep(time.Duration(timeOut) * time.Millisecond)
			rf.mu.Lock()
			if rf.roleState == Leader {
				rf.mu.Unlock()
				continue
			}
			if rf.heartbeatReceived == true {
				rf.heartbeatReceived = false
				rf.mu.Unlock()
				continue
			}
			// heartbeat lost from leader, start the election
			timeOut = Electiontimeout + (rand.Int63() % 100)
			rf.electiontimeoutms = int(timeOut)
			rf.currentTerm++
			rf.roleState = Candadtite
			rf.votedFor = rf.me
			var peerNum int32 = int32(len(rf.peers))
			rf.mu.Unlock()
			var grantedNum int32 = 1
			doneCh := make([]chan bool, len(rf.peers))
			DPrintf("node %d start election, new election timeoutms: %d", rf.me, timeOut)
			for i, _ := range rf.peers {
				doneCh[i] = make(chan bool)
				if i == rf.me {
					close(doneCh[i])
					continue
				}
				go sendRequestVoteRPC(rf, i, &grantedNum, doneCh[i])
			}
			for _,ch := range doneCh {
				<-ch
			}
			if grantedNum >= (peerNum/2 + 1) {
				DPrintf("node %d get vote from most node, term: %d", rf.me, rf.currentTerm)
				rf.mu.Lock()
				rf.roleState = Leader
				rf.mu.Unlock()
				doneCh2 := make([]chan bool, len(rf.peers))
				var failNum int32 = 0
				for i,_ := range rf.peers {
					doneCh2[i] = make(chan bool)
					if i == rf.me {
						close(doneCh2[i])
						continue
					}
					go sendAppendEntryRPC(rf, i, &failNum, doneCh2[i])
				}
				for _,ch := range doneCh {
					<-ch
				}
			} else {
				DPrintf("node %d lost vote, term: %d", rf.me, rf.currentTerm)
			}
		}
	}()

	// goroutine for sending heartbeat by leader
	go func() {
		for {
			time.Sleep(Hearteatperiod)
			rf.mu.Lock()
			if rf.roleState != Leader {
				rf.mu.Unlock()
				continue
			}
			DPrintf("leader %d sending heartbeat", rf.me)
			rf.mu.Unlock()
			var failNum int32 = 0
			var peerNum int32 = int32(len(rf.peers))
			doneCh := make([]chan bool, len(rf.peers))
			for i,_ := range rf.peers {
				doneCh[i] = make(chan bool)
				if i == rf.me {
					close(doneCh[i])
					continue
				}
				go sendAppendEntryRPC(rf, i, &failNum, doneCh[i])
			}
			for _,ch := range doneCh {
				<-ch
			}
			// lost sync with most of nodes, change to be a follower
			if failNum >= (peerNum/2 + 1) {
				DPrintf("leader %d lost sync with most of nodes, change to be a follower", rf.me)
				rf.mu.Lock()
				rf.roleState = Follwer
				rf.mu.Unlock()
			}
		}
	}()

	return rf
}
