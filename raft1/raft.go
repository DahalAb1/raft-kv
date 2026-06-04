package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.


import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

const (
    Follower  = 0
    Candidate = 1
    Leader    = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	
	// persistent state on all servers 

	// when time passes we incement currentTerm and then the leader becomes candidates
	// it will then start sending RequestVoteRPC 
	currentTerm int 
	
	// we keep track fo the leader by this string 
	votedFor int 
	role int // Foll, Cand, Lead
	lastHeard time.Time

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	
	if rf.role == Leader { 
		isleader = true 
	}

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	CandidateTerm int 
	CandidateId int 

}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).

	// did they vote or no
	// if yes then update something
	
	// if ReplyTerm > rf.CurrentTerm, the candidate is stale 
	ReplyTerm int  
	VoteGranted bool

}

// this just sends out heartbeats, so no need to worry right now
type AppendEntriesArgs struct { 
	LeaderTerm int 
	LeaderId int 
}

type AppendEntriesReply struct { 
	ReplyTerm int
	Success bool
}

// defined Jun1 
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool { 
	appendEntries := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return appendEntries
}


// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	if rf.currentTerm < args.CandidateTerm { 
		reply.VoteGranted = false 
		return 
	}	

	// totally confused about this line
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId { 
		reply.VoteGranted = true 
		rf.votedFor = args.CandidateId
	} else { 
		reply.VoteGranted = false 
	}
	
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).


	return index, term, isLeader
}




// debugging session 1 :
	/* 
	Error Occured with TestInitialElection3A --> Fatal: expected one leader, got none
	Surprising Error : The previous error stopped occuring without any changes, new error --> Warning : Data Race 
		(even with changed error my the error hypothesis remains same)


		1. Check if I'm testing this correctly 
		2. Error Hypothesis 1: the error would most probably house in ticker() or Make() 
			(Note: handle race conditions in this place)

	*/ 

func (rf *Raft) ticker() {
	for true {

		// define the leader's heartbeat here

		// no more heartbeats than 10 times a second
		if rf.role == Leader { 

			// sleep to ensure that the leader only sends 10 request every second. 
			time.Sleep(100 * time.Millisecond)
			appendRPCreq := AppendEntriesArgs{
					LeaderTerm : rf.currentTerm,
					LeaderId : rf.me, 
						} 
			appendRPCres := AppendEntriesReply{} 
			
			// run multiple go routines and then send requestRPC
			var wg sync.WaitGroup
			for peer := range rf.peers { 
				wg.Add(1)

				// only send heartbeats 10 times a second. 
				go func (p int) {
					defer wg.Done() 
					rf.sendAppendEntries(p, &appendRPCreq, &appendRPCres) 
				}(peer) 
			}
			wg.Wait()
			continue 
		}



		// Follower -> Candidate 
		// Check if a leader election should be started.
		curTime := time.Now()
		elasped := curTime.Sub(rf.lastHeard)

		if elasped.Seconds() > 5 { 

			// election timeout happened, elasped > 5 seconds 
			// wait 50 - 300 seconds to start election, to minimize 2 servers becoming candidate at the same time 
			ms := 50 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)

			// start election 
			rf.role = Candidate
			rf.votedFor = rf.me
			rf.currentTerm ++ 

			// send request vote RPC
			// should we send RequestVoteArgs in mulitiple?? idk
			reqRPC := RequestVoteArgs{ 
					CandidateTerm : rf.currentTerm, 
					CandidateId : rf.me, }
			resRPC := RequestVoteReply{}
			
			// introducing wait group to handle majorityValue which is accessed by multiple servers
			var wg sync.WaitGroup

			majorityServers := 0 

			for i := range rf.peers { 
			wg.Add(1)
			go func(i int, req RequestVoteArgs, res RequestVoteReply) {
				defer wg.Done() 
				if rf.sendRequestVote(i, &req, &res) { 
					rf.mu.Lock()
					majorityServers++
					rf.mu.Unlock()
				}
			}(i, reqRPC, resRPC)
			} 

			// wait for all the server response
			wg.Wait()

			numPeers := len(rf.peers)
			midValue := int(numPeers) / 2

			if majorityServers >= midValue { 
				rf.role = Leader
			}
			
			// start sending AppendRPC

		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0 
	rf.votedFor = -1 
	rf.role = Follower
	rf.lastHeard = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
