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

// 3B 
type LogEntry struct { 
	Command interface{}
	Term int // term when entry was recieved
	Index int 
}


// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	
	// 3A --> Election
	currentTerm int 
	votedFor int 
	role int 
	lastHeard time.Time

	// 3B --> Log Replication 
	log []LogEntry
	commitIndex int 
	lastApplied int 
	nextIndex []int
	matchIndex []int
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
	// if d.Decode(&xxx) != nil ||go test -run 3B -count=1
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
	// 3A,
	CandidateTerm int 
	CandidateId int 

	// 3B
	LastLogIndex int 
	LastLogTerm int 

}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	// if ReplyTerm > rf.CurrentTerm, the candidate is stale 
	ReplyTerm int  
	VoteGranted bool

}

type AppendEntriesArgs struct { 
	// 3A
	LeaderTerm int 
	LeaderId int
	
	// 3B 
	PrevLogIndex int 
	PrevLogTerm int 
	Entries []LogEntry 
	LeaderCommit int 
}

type AppendEntriesReply struct { 
	// 3A
	ReplyTerm int
	
	// 3B
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()

		// stale leader, here we do not reset election timer
			if args.LeaderTerm < rf.currentTerm {
				reply.ReplyTerm = rf.currentTerm

				// fig 2: Reciever Implementation, Reply false if term < currentTerm
				reply.Success = false 
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
			reply.ReplyTerm = rf.currentTerm

			
			if len(rf.log) - 1 >= args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm { 
					rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)		
					reply.Success = true 
			} else {
				reply.Success = false
			}

			// we have to keep track of the minimum commit, therefore the following
			if reply.Success && args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
			}
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

	// comparing term 
	if args.CandidateTerm < rf.currentTerm {
		reply.VoteGranted = false
		reply.ReplyTerm = rf.currentTerm
		return 
	}

	// This condition fulfills: is the candidate's log greater, and is the candidate's log most up to date? 
	// we first compare the term, if the candidate's greater then we konw it's most up to date
	// if the term is same of the candidate, we will check the size of the index to figure out if the candidate is most up to date. 
	// this preserves the log completeness property. 
	isValid := (args.LastLogTerm > rf.log[len(rf.log)-1].Term  || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log) -1))
	// can we vote? 
	voteAvaliable := (rf.votedFor == -1 || rf.votedFor == args.CandidateId) 
	// comparing log 
	if !voteAvaliable || !isValid { 
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

		if rf.role != Leader { 
			return -1,-1, false 
		}
		// the new entry takes the next free slot
		curIndx := len(rf.log)

		curLog := LogEntry{
		Command : command,
		Term : rf.currentTerm,
		Index : curIndx,
		}

		rf.log = append(rf.log, curLog)

		term = rf.currentTerm
		index = curIndx
	
	return index, term, isLeader
}



func (rf *Raft) ticker() {
	
	for {
		// pause each iteration so the follower path doesn't busy-spin
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
			role := rf.role
			term := rf.currentTerm
		rf.mu.Unlock()

		if role == Leader {
			// wait 100ms to send request.
			time.Sleep(100 * time.Millisecond)

			for peer := range rf.peers {

				// skip sending heart-beat to self
				if peer == rf.me {
					continue
				}

				// build this follower's request from its own nextIndex:
				rf.mu.Lock()
				prevIndex := rf.nextIndex[peer] - 1
				appendRPCreq := AppendEntriesArgs {
					LeaderTerm   : term,
					LeaderId     : rf.me,
					PrevLogIndex : prevIndex,
					PrevLogTerm  : rf.log[prevIndex].Term,
					Entries      : append([]LogEntry{}, rf.log[rf.nextIndex[peer]:]...),
					LeaderCommit : rf.commitIndex,
				}
				appendRPCres := AppendEntriesReply{}
				rf.mu.Unlock()
				
				


				go func (p int, req AppendEntriesArgs, res AppendEntriesReply) {
					if rf.sendAppendEntries(p, &req, &res) {

						// follower has a higher term: we are a stale leader, so update
						rf.mu.Lock()
						defer rf.mu.Unlock()

						if res.ReplyTerm > rf.currentTerm {
							rf.currentTerm = res.ReplyTerm
							rf.votedFor = -1
							rf.role = Follower
							rf.lastHeard = time.Now()
							return 
						}
						
						
						// drop replies from servers that disconnected and are still accepting past replies 
						// reply should only be valid for the term it is requesting that term in 
						// this guards against delay in client response, if leader fails and becomes leader again, and recieves old term's requests. 
						if rf.role != Leader || rf.currentTerm != req.LeaderTerm { 
							return 
						}

						if res.Success  == false { 	
								rf.nextIndex[p] -= 1 
						} else { 

							// to prevent any backward change in the leader's matchIndex 
							// some stale requests could change the matchIndex to a lower value, and it should never go backward
							rf.matchIndex[p] = max(rf.matchIndex[p], req.PrevLogIndex + len(req.Entries))
							rf.nextIndex[p] = rf.matchIndex[p] + 1 
						}

					}

				}(peer, appendRPCreq, appendRPCres)

			}

			// majority. only commit entries from the current term 
			rf.mu.Lock()
			for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
				
				// checking if the new server has recieved new log entries
				if rf.log[n].Term != rf.currentTerm {
					continue
				}

				// checking if the new server has replicated new log entries to majority
				
				count := 1 // replicated to ourselves, therefore, count ourselves
				for p := range rf.peers {
					// skip ourselves in the loop as we've already counted ourselves
					if p == rf.me { 
						continue 
					}
					
					// if matchIndex (highest replicated log entry), if log entry has been replicated, if index is equal or greater 
					// count it because we know it has been replicated
					// we are trying ot guess how many servers have the new log entries appended
					if rf.matchIndex[p] >= n {
						count++
					}
				}

				// if count is greater than the no of peers, which means if the data has been replicated on majority of servers 
				// set the commit index to n, because we know n has been committed in majority of servers
				if count > len(rf.peers)/2 {
					rf.commitIndex = n
				}
			}
			rf.mu.Unlock()

			// skips the candidate's election logic after sending heatbeats to all other server
			continue
		}


		// Follower -> Candidate 
		rf.mu.Lock()
		lastMoment := rf.lastHeard
		rf.mu.Unlock()

		curTime := time.Now()
		elasped := curTime.Sub(lastMoment)

		if elasped.Milliseconds() > 400 { 
			// wait 50 - 300 seconds to start election, to minimize 2 servers becoming candidate at the same time 
			ms := 50 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)
			
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

				// 3B 
				lastLogIndx := len(rf.log) - 1 
				lastLogTerm := rf.log[lastLogIndx].Term
			rf.mu.Unlock()

		// send request vote RPC
			reqRPC := RequestVoteArgs{ 
					CandidateTerm : term, 
					CandidateId : rf.me,

					// 3B 
					LastLogIndex: lastLogIndx,
					LastLogTerm: lastLogTerm,
				}
			resRPC := RequestVoteReply{}
			

				noPeers := len(rf.peers)
				ch := make(chan int, noPeers)
				done := make(chan struct{})

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
					if votes >= (noPeers/2)+1 {
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
					// when the candidate becomes the leader we'd have to
					// re initialize rf.nextIndex[i] and rf.matchIndex[i]

					for i:= range rf.peers { 
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0 
					}
				}
				rf.mu.Unlock()
			}

			// tell leftover vote goroutines to stop
			close(done)
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
	rf          := &Raft{}
	rf.peers     = peers
	rf.persister = persister
	rf.me        = me

	// Your initialization code here (3A, 3B, 3C).
	
	// 3A 
	rf.currentTerm = 0 
	rf.votedFor    = -1 
	rf.role        = Follower
	rf.lastHeard   = time.Now()

	// 3B 
	rf.log         = []LogEntry{{Term : 0, Index : 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex   = make([]int, len(rf.peers))
	rf.matchIndex  = make([]int, len(rf.peers))

	// initially have nextIndex and matchIndex
	for p := range peers{ 
		rf.nextIndex[p] = len(rf.log) 
		rf.matchIndex[p] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go func(){
		// apply data to the state machine 
		for true { 
			rf.mu.Lock()
			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied ++ 
				cmd := rf.log[rf.lastApplied].Command
				indx := rf.log[rf.lastApplied].Index
				rf.mu.Unlock()

				// applyCh is unbufferece, a secnd on it waits until someone on the other end takes the value. 
				applyCh <- raftapi.ApplyMsg{CommandValid: true, Command: cmd, CommandIndex: indx}
			} else { 
				rf.mu.Unlock()

				// nothing new to apply. without a pause this loop would re-check the
				// same thing millions of times a second and hog the CPU. nap 10ms,
				// then check again.
				time.Sleep(10 * time.Millisecond)
			}}
	}()
	return rf
}
