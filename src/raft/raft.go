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
	"bytes"
	"com.example.mit6_824/src/labgob"
	"com.example.mit6_824/src/labrpc"
	"log"
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
	Electiontimeoutbase = 300// time.Millisecond
	Electiontimeoutquota = 250// time.Millisecond
	Hearteatperiod = 200 *time.Millisecond
	RPCTtimeout = 100 * time.Millisecond
	Logapplyperiod = 150 * time.Millisecond
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

type LogEntry struct {
	INDEX int
	TERM int
	COMMAND interface{}
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

	// persistent state for all servers in paper
	currentTerm int
	votedFor int
	log []LogEntry

	// volatile state for all servers
	roleState int
	logIndex int // start from 1
	// in paper
	commitIndex int  // start from 1
	lastApplied int  // start from 1

	// volatile state for leader in paper
	nextIndex []int
	matchIndex []int

	// extra.
	// for followers
	heartbeatReceived bool
	// for debug
	electiontimeoutms int
	// for config test
	applyCh chan ApplyMsg
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
//lock is needed before call this function
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	data := writer.Bytes()
	rf.persister.SaveRaftState(data)
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
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	var currentTerm int
	var votedFor int
	var logEntry[]LogEntry
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&logEntry) != nil {
		log.Printf("node %d read persist error", rf.me)
		return
	}
	rf.mu.Lock()
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = logEntry
	rf.logIndex = len(rf.log)
	rf.mu.Unlock()
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	TRACEID int
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
		if len(rf.log) > 0 {
			if args.LASTLOGTERM < rf.log[len(rf.log)-1].TERM ||
				(args.LASTLOGTERM == rf.log[len(rf.log)-1].TERM &&
					args.LASTLOGINDEX < rf.log[len(rf.log)-1].INDEX) {
				reply.VOTEGRANTED = false
				reply.TERM = rf.currentTerm
				// let newer log node have more newer term
				if args.TERM > rf.currentTerm {
					rf.currentTerm = args.TERM
				}
				rf.mu.Unlock()
				DPrintf("node %d reject vote from %d, log not new enough, traceId: %d", rf.me, args.CANDIDATEID, args.TRACEID)
				return
			}
		}
		if (rf.votedFor == -1 || rf.votedFor == args.CANDIDATEID) ||
			args.TERM > rf.currentTerm {
			DPrintf("node %d voted for %d, change state %d to a follower, traceId: %d", rf.me, args.CANDIDATEID, rf.roleState, args.TRACEID)
			rf.votedFor = args.CANDIDATEID
			rf.currentTerm = args.TERM
			rf.roleState = Follwer
			rf.heartbeatReceived = true
			reply.TERM = rf.currentTerm
			reply.VOTEGRANTED = true
			rf.persist()
		} else {
			DPrintf("node %d reject vote from %d, the vote already for %d, traceId: %d", rf.me, args.CANDIDATEID, rf.votedFor, args.TRACEID)
			reply.TERM = rf.currentTerm
			reply.VOTEGRANTED = false
		}
	} else {
		DPrintf("node %d reject vote from %d, the candidate term %d is older than current term %d, traceId: %d", rf.me, args.CANDIDATEID, args.TERM, rf.currentTerm, args.TRACEID)
		reply.TERM = rf.currentTerm
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
	TRACEID int
	TERM int
	LEADERID int
	PREVLOGINDEX int
	PREVLOGTERM int
	ENTRIES []LogEntry
	LEADERCOMMIT int
}

type AppendEntriesReply struct {
	TERM int
	SUCCESS bool
}

/*
find a log entry in current server as specifies index and term,
return true if exist, return false if not.
 */
func findLogEntry(rf *Raft, index int, term int) bool {
	if index == 0 {
		return true
	}
	for _,log := range rf.log {
		if log.INDEX == index && log.TERM == term {
			return true
		}
	}
	return false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.TERM < rf.currentTerm {
		DPrintf("node %d reject append entry rpc from %d, leader term %d older than current term %d, traceId: %d", rf.me, args.LEADERID, args.TERM, rf.currentTerm, args.TRACEID)
		reply.SUCCESS = false
		reply.TERM = rf.currentTerm
	} else {
		rf.heartbeatReceived = true
		if len(args.ENTRIES) > 0 {
			if findLogEntry(rf, args.PREVLOGINDEX, args.PREVLOGTERM) == true {
				for i:=0;i<len(args.ENTRIES);i++ {
					if args.PREVLOGINDEX + i < len(rf.log) {
						if rf.log[args.PREVLOGINDEX + i].TERM != args.ENTRIES[i].TERM {
							rf.log = append(rf.log[:args.PREVLOGINDEX + i],
								args.ENTRIES[i:]...)
							break
						}
					} else {
						rf.log = append(rf.log, args.ENTRIES[i:]...)
						break
					}
				}

				rf.logIndex = len(rf.log)
				if args.LEADERCOMMIT > rf.commitIndex {
					rf.commitIndex = minIndex(args.LEADERCOMMIT, rf.log[len(rf.log)-1].INDEX)
				}
				DPrintf("node %d received log: %+v, new commitIndex: %d, traceId: %d", rf.me, args.ENTRIES, rf.commitIndex, args.TRACEID)
				reply.SUCCESS = true
				reply.TERM = rf.currentTerm
			} else {
				DPrintf("node %d reject append entry rpc from %d, leader prev log, index: %d, term: %d not found, traceId: %d", rf.me, args.LEADERID, args.PREVLOGINDEX, args.PREVLOGTERM, args.TRACEID)
				reply.SUCCESS = false
				reply.TERM = rf.currentTerm
			}
		} else {
			if len(rf.log) > 0 {
				if args.LEADERCOMMIT > rf.commitIndex {
					rf.commitIndex = minIndex(args.LEADERCOMMIT, rf.log[len(rf.log)-1].INDEX)
				}
			}

			reply.SUCCESS = true
			reply.TERM = rf.currentTerm
		}
	}
	if reply.SUCCESS == true {
		if rf.currentTerm < args.TERM {
			rf.currentTerm = args.TERM
			// swicth to new term, reset vote
			rf.votedFor = -1
		}
		if rf.roleState != Follwer {
			DPrintf("node %d received append entry rpc from leader %d, change to be a follower, traceId: %d", rf.me, args.LEADERID, args.TRACEID)
			rf.roleState = Follwer
		}
		rf.persist()
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
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.roleState == Leader
	if rf.roleState == Leader {
		rf.logIndex++
		logEntry := LogEntry{
			INDEX: rf.logIndex,
			TERM: rf.currentTerm,
			COMMAND: command,
		}
		rf.log = append(rf.log, logEntry)
		index = rf.logIndex
		DPrintf("leader %d received new log: %+v, new logIndex: %d", rf.me, logEntry, rf.logIndex)
		rf.persist()
	}
	rf.mu.Unlock()

	if isLeader == true {
		sendAppendEntryToPeers(rf)
	}

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

func (rf *Raft) CheckLogExist(index int, term int) bool {
	if index < 1 {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.log) >= index && rf.log[index-1].TERM == term {
		return true
	}
	return false
}

func sendRequestVoteRPC(rf *Raft, server int, grantNum *int32, netFailed *int32, ch chan bool) {
	traceId := rand.Int()
	args := &RequestVoteArgs{
		TERM: rf.currentTerm,
		CANDIDATEID: rf.me,
		TRACEID: traceId,
	}

	if len(rf.log) > 0 {
		args.LASTLOGINDEX = rf.log[len(rf.log)-1].INDEX
		args.LASTLOGTERM = rf.log[len(rf.log)-1].TERM
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
	case sendOk := <-doneCh:
		if sendOk == false {
			atomic.AddInt32(netFailed, 1)
			DPrintf("node %d send vote request rpc to %d net failed, traceId: %d", rf.me, server, traceId)
		} else if reply.VOTEGRANTED == true {
			atomic.AddInt32(grantNum, 1)
		} else {
			/*
			rf.mu.Lock()
			if rf.currentTerm < reply.TERM {
				rf.currentTerm = reply.TERM
				rf.persist()
			}
			rf.mu.Unlock()

			 */
		}
	case <-time.After(RPCTtimeout):
		atomic.AddInt32(netFailed, 1)
		DPrintf("node %d send vote request rpc to %d timeout, traceId: %d", rf.me, server, traceId)
	}
	close(ch)
}

func sendRequestVoteToPeers(rf *Raft) {
	rf.mu.Lock()
	var peerNum int32 = int32(len(rf.peers))
	var grantedNum int32 = 1
	var netFailed int32 = 0
	doneCh := make([]chan bool, len(rf.peers))
	for i, _ := range rf.peers {
		doneCh[i] = make(chan bool)
		if i == rf.me {
			close(doneCh[i])
			continue
		}
		go sendRequestVoteRPC(rf, i, &grantedNum, &netFailed, doneCh[i])
	}
	for _,ch := range doneCh {
		<-ch
	}
	needSendAppentEntry := false
	if grantedNum >= (peerNum/2 + 1) {
		// check if node voted for other node in waiting for rpc
		if rf.roleState != Candadtite {
			rf.mu.Unlock()
			return
		}
		DPrintf("node %d get vote from most node, term: %d", rf.me, rf.currentTerm)
		rf.roleState = Leader
		needSendAppentEntry = true
	} else {
		if netFailed >= (peerNum/2 + 1) {
			DPrintf("node %d lost vote because network loss, term: %d, restore invalid term increment", rf.me, rf.currentTerm)
			rf.currentTerm--
		} else {
			DPrintf("node %d lost vote, term: %d", rf.me, rf.currentTerm)
		}
	}
	rf.mu.Unlock()
	if needSendAppentEntry == true {
		sendAppendEntryToPeers(rf)
	}
}

// need Raft lock outside
func sendAppendEntryRPC(rf *Raft, server int, rpcFailed *int32, netFailed *int32, ch chan bool) {
	success := false
	traceId := rand.Int()

	args := &AppendEntriesArgs{
		TERM:         rf.currentTerm,
		LEADERID:     rf.me,
		LEADERCOMMIT: rf.commitIndex,
		TRACEID: traceId,
	}

	if rf.logIndex >= rf.nextIndex[server] {
		args.ENTRIES = rf.log[rf.nextIndex[server]-1 : rf.logIndex]
	} else {
		rf.nextIndex[server] = rf.logIndex + 1
	}
	if rf.nextIndex[server] >= 2 {
		index := rf.nextIndex[server]-2
		args.PREVLOGINDEX = rf.log[index].INDEX
		args.PREVLOGTERM = rf.log[index].TERM
	} else {
		args.PREVLOGINDEX = 0
		args.PREVLOGTERM = 0
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
	case sendOk := <-doneCh:
		if sendOk == false {
			DPrintf("leader %d lost connection with node %d, traceId: %d", rf.me, server, traceId)
			atomic.AddInt32(netFailed, 1)
		} else if reply.SUCCESS == false {
			DPrintf("leader %d send append entry rpc failed to node %d, traceId: %d", rf.me, server, traceId)
			if rf.nextIndex[server] > 1 {
				rf.nextIndex[server]--
			}
			atomic.AddInt32(rpcFailed, 1)
		} else {
			rf.nextIndex[server] += len(args.ENTRIES)
			rf.matchIndex[server] = rf.nextIndex[server]-1
			success = true
		}
	case <-time.After(RPCTtimeout):
		DPrintf("leader %d connection with node %d timeout, traceId: %d", rf.me, server, traceId)
		atomic.AddInt32(netFailed, 1)
	}
	ch <- success
}

/*
return:
	first, rpc request failed number
	second, network failed number
 */
func sendAppendEntryToPeers(rf *Raft) (int32, int32) {
	rf.mu.Lock()
	doneCh := make([]chan bool, len(rf.peers))
	success := make([]bool, len(rf.peers))
	var rpcFailed int32 = 0
	var netFailed int32 = 0
	for i,_ := range rf.peers {
		doneCh[i] = make(chan bool, 1)
		if i == rf.me {
			doneCh[i] <- true
			continue
		}
		go sendAppendEntryRPC(rf, i, &rpcFailed, &netFailed, doneCh[i])
	}
	for i,ch := range doneCh {
		success[i] = <- ch
	}

	if int(rpcFailed + netFailed) < (len(rf.peers)/2 + 1) {
		var validIndex []int
		for i,res := range success {
			if i == rf.me {
				validIndex = append(validIndex, len(rf.log))
				continue
			}
			if res == true {
				validIndex = append(validIndex, rf.matchIndex[i])
			}
		}
		commitIndex := findNthMinIndex(validIndex, 0)
		if commitIndex > 0 && len(rf.log) > 0 && rf.log[commitIndex-1].TERM == rf.currentTerm &&
			commitIndex > rf.commitIndex {
			rf.commitIndex = commitIndex
			DPrintf("leader %d commitIndex updated to %d, log: %+v", rf.me, rf.commitIndex, rf.log)
		}
	}
	rf.mu.Unlock()
	return rpcFailed, netFailed
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
	// log index start from 1, 0 means nothing commit or applied
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	var nextIndex int = 1
	if len(rf.log) > 0 {
		nextIndex = len(rf.log)+1
	}
	for i:=0;i<len(rf.peers);i++ {
		rf.nextIndex[i] = nextIndex
	}
	// goroutine for leader election
	timeOut := Electiontimeoutbase + (rand.Int63() % Electiontimeoutquota)
	rf.electiontimeoutms = int(timeOut)
	DPrintf("node %d init election timeoutms: %d", rf.me, timeOut)
	go func() {
		for {
			time.Sleep(time.Duration(timeOut) * time.Millisecond)
			if rf.killed() {
				break
			}
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
			timeOut = Electiontimeoutbase + (rand.Int63() % Electiontimeoutquota)
			rf.electiontimeoutms = int(timeOut)
			rf.currentTerm++
			rf.roleState = Candadtite
			rf.votedFor = rf.me
			rf.persist()
			rf.mu.Unlock()
			DPrintf("node %d start election, new election timeoutms: %d", rf.me, timeOut)
			sendRequestVoteToPeers(rf)
			// reset vote for next election
			rf.mu.Lock()
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()
		}
	}()

	// goroutine for sending heartbeat by leader
	go func() {
		for {
			time.Sleep(Hearteatperiod)
directly_append:
			if rf.killed() {
				break
			}
			rf.mu.Lock()
			if rf.roleState != Leader {
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()
			var peerNum int32 = int32(len(rf.peers))
			rpcFailed, netFailed := sendAppendEntryToPeers(rf)
			// lost sync with most of nodes, change to be a follower
			if netFailed >= (peerNum/2 + 1) {
				DPrintf("leader %d lost connection with most of nodes, change to be a follower", rf.me)
				rf.mu.Lock()
				rf.roleState = Follwer
				rf.mu.Unlock()
			} else if rpcFailed+netFailed >= (peerNum/2 + 1) {
				goto directly_append
			}
		}
	}()

	// goroutine for log apply
	go func() {
		for {
			time.Sleep(Logapplyperiod)
directly_apply:
			if rf.killed() {
				break
			}
			rf.mu.Lock()
			if rf.commitIndex > rf.lastApplied && rf.lastApplied < len(rf.log) {
				rf.lastApplied++
				msg := ApplyMsg{
					CommandValid: true,
					CommandIndex: rf.log[rf.lastApplied-1].INDEX,
					Command: rf.log[rf.lastApplied-1].COMMAND,
				}
				rf.applyCh <- msg
				rf.mu.Unlock()
				goto directly_apply
			}
			rf.mu.Unlock()
		}
	}()
	return rf
}
