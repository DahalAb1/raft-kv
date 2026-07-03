package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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
	electionDeadline time.Time // next moment the ticker may campaign; re-drawn randomly on every reset
	dead int32 // set by Kill(), read by killed(); atomic so no lock needed

	// 3B --> Log Replication 
	log []LogEntry
	commitIndex int 
	lastApplied int 
	nextIndex []int
	matchIndex []int

	// 3C --> Persistance 
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// take the persistent states, and save these into w
	per_CurTerm := rf.currentTerm
	per_VoteFor := rf.votedFor 
	per_log 	:= rf.log


	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	e.Encode(per_CurTerm)
	e.Encode(per_VoteFor)
	e.Encode(per_log)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)

}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

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

	// Previously persisted state should get the stored value from memory 
	// after getting the values store it into the raft's structure 
	// persistent states are : 
	// currentTerm, votedFor, log[] 
	
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var per_CurTerm int 
	var per_VoteFor int 
	var per_Log []LogEntry 

	gotCurTerm := d.Decode(&per_CurTerm)
	gotVotedFor := d.Decode(&per_VoteFor)
	gotLog := d.Decode(&per_Log)

	if gotCurTerm 	!= nil || 
	   gotVotedFor  != nil || 
	   gotLog 		!= nil {
	
			  // We land here when Decode returned an error for given data []byte.
              // That means: the file had data, but we could not turn it back into
              // currentTerm / votedFor / log. The data is broken.
              // If we took the currpted data, this server would start fresh
              // with term 0 and no vote, as if it never voted. 
			  // A server like that can vote a second time in a term it already voted in, and two leaders can win, which is a big problem. 
              //
              // We do not proceed in this case. We handle this error by building one message saying which decode failed, and panic.
              // Panic stops the program and prints a stack trace, so we see
              // exactly where it died.

			// panic will be really helpful when we are debugging: it fails the test
            // right away instead of letting bad state cause a weird failure much
            // later, like two different votes from a server in the same term.
		
		
		panic(fmt.Sprintf("readPersist: decode failed (currentTerm: %v, votedFor: %v, log: %v)", gotCurTerm, gotVotedFor, gotLog))

	} else {
		rf.currentTerm = per_CurTerm
        rf.votedFor = per_VoteFor
        rf.log = per_Log
    }
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

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call Kill(). your code can use killed() to check whether
// Kill() has been called. the use of atomic avoids the need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// push the election deadline out by a fresh random 400-700ms. the randomness
// must live in the deadline itself, drawn again on every reset — if all
// servers shared a fixed threshold they'd cross it together (they boot at the
// same instant) and only a post-threshold sleep would separate them, which
// lets two servers campaign within an RPC's travel time of each other.
// caller must hold rf.mu.
func (rf *Raft) resetElectionTimer() {
	ms := 400 + (rand.Int63() % 300)
	rf.electionDeadline = time.Now().Add(time.Duration(ms) * time.Millisecond)
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

	// 3C --> fast backup (paper §5.3: "include the term of the conflicting
	// entry and the first index it stores for that term")
	XTerm int  // term of the conflicting entry at PrevLogIndex, -1 if log too short
	XIndex int // first index the follower stores for XTerm
	XLen int   // follower's log length, used when XTerm == -1
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
			rf.resetElectionTimer()
			reply.ReplyTerm = rf.currentTerm

			
			if len(rf.log) - 1 >= args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm { 
					// instead of trunctate lets walk 

					// I had initially implemented this condition by simply truncating the value, which was obvious becuase I had wrong notion, My belief
					// was that if something was sent serieall through the network, the reciever would get it serially as well, which is completely wrong. i've described why truncate does not work. : 
					/**
						Network connection is flaky, sometimes we recieve data serially, sometimes we
						don't. When we run go routines, 2 different RPC's A and B, where A was sent to
						the follower before B, B could arrive faster than A. This shows us that we
						shoud not Trust network connection. In this specific situation if B arrives
						before A and if B has data that was not present in A, then B would open this
						condition and then change the log's value. The log grows to a new last index
						and could be committed by the leader, and applied to the state machine. Now
						what if A arrives, after B's RPC was committed, and B's data has applied
						command to state machine. That would be a problem, as the RPC will trigger
						this condition because the index and the term is correct — and the truncate
						below would delete the committed entry from our log. A committed entry must
						never be lost.

						To avoid this problem we take steps to replicate log from the leader to the
						follower. We look at every log entry from previousIndex, and only remove when
						there's a real conflict (same index, different term), and only add entries we
						are missing. Same index and same term means we already have it, so we touch
						nothing — a stale shorter RPC becomes a no-op. This will ensure committed data
						stays consistent forever, not just within the term.
					**/
					
					
					// we'll start from PrevLogIndex+1 (Entries[0]'s slot), one slot per entry.
					for i := args.PrevLogIndex + 1; i <= args.PrevLogIndex + len(args.Entries); i++ {
						entryTerm := args.Entries[i - args.PrevLogIndex - 1].Term

						// slot doesn't exist: our log ends here, append the rest and stop
						if i >= len(rf.log) {
							rf.log = append(rf.log, args.Entries[i - args.PrevLogIndex - 1:]...)
							break
						}

						// same index, same term: we already have it, touch nothing
						if rf.log[i].Term == entryTerm {
							continue
						}

						// conflict: same index, different term. everything from here on
						// is a dead branch — truncate it and take the leader's entries
						rf.log = append(rf.log[:i], args.Entries[i - args.PrevLogIndex - 1:]...)
						break
					}

					reply.Success = true


			} else {
				reply.Success = false

				// fast backup: tell the leader enough to skip a whole term of
				// conflicting entries per RPC instead of walking back one by one
				if len(rf.log) - 1 < args.PrevLogIndex {
					// our log is too short to even have PrevLogIndex,
					// leader should restart from our log's end
					reply.XTerm = -1
					reply.XLen = len(rf.log)
				} else {
					// we have an entry at PrevLogIndex but its term conflicts.
					// report that term and walk back to the first index we
					// store for it — everything in that run is the same dead
					// branch, so the leader can jump over all of it at once
					reply.XTerm = rf.log[args.PrevLogIndex].Term
					i := args.PrevLogIndex
					for i > 1 && rf.log[i-1].Term == reply.XTerm {
						i--
					}
					reply.XIndex = i
				}
			}

			// we have to keep track of the minimum commit, therefore the following.
			// clamp to the last entry THIS RPC vouched for — our log may keep a longer
			// tail the leader said nothing about, and we must not commit into it
			if reply.Success && args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex + len(args.Entries))
			}

			// persist #1 
			rf.persist()
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

		// persist here, not just on the grant path below: if the vote is
		// rejected we return early, but currentTerm/votedFor already changed
		// and the reply carries the new term. crash after replying would
		// reboot us at the old term, disagreeing with what we told the world.
		rf.persist()
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


		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimer()

	reply.ReplyTerm = rf.currentTerm
	
	// persist #2 
	rf.persist()
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
		
		// persist # 3
		rf.persist()
	return index, term, isLeader
}



func (rf *Raft) ticker() {

	for !rf.killed() {
		// pause each iteration so the follower path doesn't busy-spin
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
			role := rf.role
			term := rf.currentTerm
		rf.mu.Unlock()

		if role == Leader {

			for peer := range rf.peers {

				// skip sending heart-beat to self
				if peer == rf.me {
					continue
				}

				// build this follower's request from its own nextIndex:
				rf.mu.Lock()

				// we may have stepped down between the role snapshot at the top of
				// the tick and this lock (a reply carried a higher term), and our own
				// AppendEntries handler may have truncated rf.log since. building args
				// from nextIndex here could then index past the end of the log and
				// panic — and would send a stale-term RPC besides. re-check first.
				if rf.role != Leader || rf.currentTerm != term {
					rf.mu.Unlock()
					break
				}

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
							rf.resetElectionTimer()
							rf.persist()
							return
						}
						
						
						// drop replies from servers that disconnected and are still accepting past replies 
						// reply should only be valid for the term it is requesting that term in 
						// this guards against delay in client response, if leader fails and becomes leader again, and recieves old term's requests. 


						// A stale reply from a follower can incorrectly cause the leader to update nextIndex and matchIndex. 
						// The leader may then believe that the follower has a valid copy of the current log entry, 
						// even though the reply only confirmed an old request from a previous term.
						if rf.role != Leader || rf.currentTerm != req.LeaderTerm { 
							return 
						}

						if res.Success  == false {
							// fast backup: skip a whole conflicting term per RPC
							// instead of stepping nextIndex back one entry at a time
							if res.XTerm == -1 {
								// follower's log is shorter than PrevLogIndex,
								// resume from its end
								rf.nextIndex[p] = res.XLen
							} else {
								// jump to the follower's first entry of the
								// conflicting term...
								rf.nextIndex[p] = res.XIndex

								// ...unless we also have entries of that term:
								// those match, so start right after our last one
								for i := len(rf.log) - 1; i > 0; i-- {
									if rf.log[i].Term == res.XTerm {
										rf.nextIndex[p] = i + 1
										break
									}
								}
							}
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
			

			// sleep for 100ms before sending another AppendRPC, the lab only accpets appendRPC 10 times per second. 
			time.Sleep(100 * time.Millisecond)
			// skips the candidate's election logic after sending heatbeats to all other server
			continue
		}


		// Follower -> Candidate
		rf.mu.Lock()
		if time.Now().Before(rf.electionDeadline) {
			rf.mu.Unlock()
			continue
		}

		// deadline passed without hearing from a leader — start election.
		// no pre-election sleep and no re-check anymore: the randomness lives
		// in the deadline itself, and the check + state change happen under
		// one lock hold, so a heartbeat can't sneak in between.
			rf.role = Candidate
			rf.votedFor = rf.me
			rf.currentTerm ++
			term = rf.currentTerm
			rf.resetElectionTimer()

			// 3B
			lastLogIndx := len(rf.log) - 1
			lastLogTerm := rf.log[lastLogIndx].Term

			// persist #9 
			rf.persist()
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
						rf.persist()
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
	// each server boots with its own random 400-700ms deadline — this is what
	// breaks the boot-time synchronization (all five used to share now+400ms)
	rf.resetElectionTimer()

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
		for !rf.killed() {
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
