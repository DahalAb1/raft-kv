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

	// debugger error in this function as well 
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (3A).

	term = rf.currentTerm
	
	// data race here as well? 
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

			rf.mu.Lock()
			defer rf.mu.Unlock()

		// stale leader, here we do not reset election timer
			if args.LeaderTerm < rf.currentTerm {
				reply.Success = false
				reply.ReplyTerm = rf.currentTerm
				return
			}

		// valid leader, become follower
			if args.LeaderTerm > rf.currentTerm {
				rf.currentTerm = args.LeaderTerm
				rf.votedFor = -1
			}
			rf.role = Follower

		// heard from the leader, reset the election timer
			rf.lastHeard = time.Now()

			reply.Success = true
			reply.ReplyTerm = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool { 
	appendEntries := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return appendEntries
}


// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.CandidateTerm > rf.currentTerm {
		rf.currentTerm = args.CandidateTerm
		rf.votedFor = -1
		rf.role = Follower
	}

	if args.CandidateTerm < rf.currentTerm {
		reply.VoteGranted = false
		reply.ReplyTerm = rf.currentTerm
		return 
	}
	
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {

		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastHeard = time.Now()
		
	} else {
		// already voted for someone else this term, rf.votedFor != args.CandidateId
		reply.VoteGranted = false
	}

	reply.ReplyTerm = rf.currentTerm
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


		1. Check if I'm testing this correctly (done, conclusion : my testing method is robust and valid as per the instructions of the course) 
		2. Error Hypothesis 1: the error would most probably house in ticker() or Make() 
			(Note: handle race conditions in this place)

	*/ 

func (rf *Raft) ticker() {
	
	for true {
		
		// Data Race Speculations 
			// rf.role --> AppendRPC is triggered, but this loop also is triggered


		// This is a really smart way to fix it
		rf.mu.Lock()
			role := rf.role 
			term := rf.currentTerm
		rf.mu.Unlock()

		if role == Leader { 
			
			// wait 100ms to send request. 
			time.Sleep(100 * time.Millisecond)
			
			appendRPCreq := AppendEntriesArgs {
					LeaderTerm : term,
					LeaderId : rf.me, 
						} 

			appendRPCres := AppendEntriesReply{} 
			

			var wg sync.WaitGroup
			for peer := range rf.peers { 

				// no need to send heart beat to myself
				// but I believe there's no error if we send heartBeat to ourselves
				if peer == rf.me { 
					continue 
				}

				wg.Add(1)

				// missed this before did not pass in copies, directly passed response
				// We have to pass in copies of appendRPC otherwise everything will write itself in a single response struct
				go func (p int, req AppendEntriesArgs, res AppendEntriesReply) {
					// decrements wait group when the funciton is done
					defer wg.Done()
					if rf.sendAppendEntries(p, &req, &res) {

						// follower has a higher term: we are a stale leader,
						// step down (paper Figure 2)
						rf.mu.Lock()
						if res.ReplyTerm > rf.currentTerm {
							rf.currentTerm = res.ReplyTerm
							rf.votedFor = -1
							rf.role = Follower
							rf.lastHeard = time.Now()
						}
						rf.mu.Unlock()
					}

				}(peer, appendRPCreq, appendRPCres)

			}
			wg.Wait()

			// skips the candidate's election logic after sending heatbeats to all other server 

			continue 
		}






		// for a candidate what are the things that I'd have to handle? 
		/* 
			1. If it's a follower and time elasped is greater than make it candidate and send RequestRPC
			2. What if it's already a candidate, prompt to send RequestRPC 
			3. If we have a reply index greater than the candidate then turn off the election and return the candidate back to follower 
			4. handle other followers request
			5. The following case is for a follower, what if the node is a candidate itself? 
			6. I've not implemented timers, where and how to implement it what even is it? 

		*/

		// Follower -> Candidate 
		
		rf.mu.Lock()
		lastMoment := rf.lastHeard
		rf.mu.Unlock()

		curTime := time.Now()
		elasped := curTime.Sub(lastMoment)

		if elasped.Milliseconds() > 400 { 
			// there's no reason i put it to 400... 
			// a follower starts election when it has not gotten 4 heartbeats, each heartbeat is sent out every 100ms, 10 a second



			// wait 50 - 300 seconds to start election, to minimize 2 servers becoming candidate at the same time 
			ms := 50 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)



			// when could we trigger this case as false? 
			// if it was a candidate and the server crashed, then it will try to start an election
			// if rf.role != Candidate {  // i don't think this check is necessary 
			// RequestVote rpc will handle this, as it will reject the vote and this will fall back to follower
				
			
		// start election
			rf.mu.Lock()
				// a heartbeat may have arrived during the random sleep above —
				// re-check before electing, so we don't disrupt a valid leader
				if time.Since(rf.lastHeard).Milliseconds() < 400 {
					rf.mu.Unlock()
					continue
				}
				rf.role = Candidate
				rf.votedFor = rf.me
				rf.currentTerm ++
				term:= rf.currentTerm
			rf.mu.Unlock()


		// send request vote RPC
			reqRPC := RequestVoteArgs{ 
					CandidateTerm : term, 
					CandidateId : rf.me, 
				}
			resRPC := RequestVoteReply{}
			

		// Here wait group is necessary to ... 
			// var wg sync.WaitGroup

			// majorityServers := 1 
			
			// locking here to create no of channels, here i've put safety to ensure that changes in rf.peers won't affect our election
			rf.mu.Lock()
				noPeers := len(rf.peers)
				ch := make(chan int, noPeers)
			rf.mu.Unlock() 
				done := make(chan struct{})

			// race condition here? rf.peers can change, what will happen if one of the peers breaks? 
			for i := range rf.peers { 

				// if waiting for vote, append RPC is encountered then we stop the vote. 
				rf.mu.Lock()
				if rf.role == Follower { 
					rf.mu.Unlock()
					break 
				}
				rf.mu.Unlock()



				if i == rf.me {
					continue 
				}
			
			//wg.Add(1)
			go func(i int, req RequestVoteArgs, res RequestVoteReply) {
				if rf.sendRequestVote(i, &req, &res) {

					// reply carries a higher term: we are stale, step down
					// (paper Figure 2: if response term T > currentTerm,
					// set currentTerm = T and convert to follower)
					rf.mu.Lock()
					if res.ReplyTerm > rf.currentTerm {
						rf.currentTerm = res.ReplyTerm
						rf.votedFor = -1
						rf.role = Follower
					}
					rf.mu.Unlock()

					if res.VoteGranted {
						select {
						case ch <- res.ReplyTerm: // send vote, election still active
						case <-done:              // election ended, drop vote
						}
					}
				}
			}(i, reqRPC, resRPC)


		    } 
			
			// count votes until majority, loss, or timeout
			votes := 1 // voted for self
			won := false
			timeout := time.After(300 * time.Millisecond)

		countLoop:
			for {
				select {
				case <-ch: // a vote arrived
					votes++
					if votes >= noPeers/2+1 {
						won = true
						break countLoop
					}
				case <-timeout: // election took too long, give up and retry next tick
					break countLoop
				}

				// lost election? (a new leader's AppendEntries made us Follower)
				rf.mu.Lock()
				if rf.role == Follower {
					rf.mu.Unlock()
					break countLoop
				}
				rf.mu.Unlock()
			}

			if won {
				rf.mu.Lock()
				if rf.role == Candidate {
					rf.role = Leader
				}
				rf.mu.Unlock()
			}

			// tell leftover vote goroutines to stop
			close(done)
			
			// to sovle the eleciton problem 
			// 1. How do i know how many servers are active as candidate 
			// 2. How many votes do i need to become a leader

			// wait for all the server response
			// i don't think we need to wait, we need to wait until we reach a certain no of servers and then we can stop

			// implementation of wait group here is not valid
			 
			//wg.Wait()

			// if i remove we.Wait(), it will be chaotic, 
			


			// numPeers := len(rf.peers)
			// midValue := numPeers / 2

			// if majorityServers > midValue { 
			// 	rf.mu.Lock()
			// 	if rf.role == Candidate { 
			// 		rf.role = Leader
			// 	} 
			// 	rf.mu.Unlock()
			// }

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

	// error here as well 
	rf.lastHeard = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections

	// race condition here as well?? 6.5840/raft1.Make.gowrap1()
	go rf.ticker()


	return rf
}
