package raft

// The consensus core: leader election, log replication, persistence,
// and snapshots. ../raftapi/raftapi.go defines the interface this file
// exposes to the service above; Make() creates a peer implementing it.

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"raft-kv/labgob"
	"raft-kv/labrpc"
	"raft-kv/raftapi"
	"raft-kv/tester"
)

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

type LogEntry struct {
	Command interface{}
	Term    int // term when the entry was received
	Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// election state
	currentTerm      int
	votedFor         int
	role             int
	electionDeadline time.Time // next moment the ticker may campaign; re-drawn randomly on every reset
	dead             int32     // set by Kill(), read by killed(); atomic so no lock needed

	// log replication state
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	// snapshot state
	pendingSnapshot  []byte
	pendingSnapIndex int
	pendingSnapTerm  int

	// speed: Start() kicks this so the leader loop replicates the
	// new entry immediately instead of finishing its 100ms heartbeat sleep
	startKick chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool

	term = rf.currentTerm

	if rf.role == Leader {
		isleader = true
	}

	return term, isleader
}

// save Raft's persistent state — currentTerm, votedFor, and the log,
// Figure 2's triple — to stable storage. called after the last mutation
// and before the mutex is released, so nobody can observe a change that
// is not yet on disk. Save() rewrites both slots of the disk, so the
// existing snapshot is passed through — nil here would erase it.
// caller must hold rf.mu.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	per_CurTerm := rf.currentTerm
	per_VoteFor := rf.votedFor
	per_log := rf.log

	e.Encode(per_CurTerm)
	e.Encode(per_VoteFor)
	e.Encode(per_log)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var per_CurTerm int
	var per_VoteFor int
	var per_Log []LogEntry

	gotCurTerm := d.Decode(&per_CurTerm)
	gotVotedFor := d.Decode(&per_VoteFor)
	gotLog := d.Decode(&per_Log)

	if gotCurTerm != nil ||
		gotVotedFor != nil ||
		gotLog != nil {

		// the disk had data we couldn't decode. proceeding would boot this
		// server fresh — term 0, no vote — and a server that forgets its
		// vote can vote twice in the same term, electing two leaders.
		// fail loudly, naming the decode that broke, instead.
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// ignore points already inside the snapshot or not yet committed
	offset := rf.log[0].Index
	if index <= offset || index > rf.commitIndex {
		return
	}

	// copy into a fresh slice so the old backing array — the burned
	// entries — can actually be garbage collected. slot 0 becomes the
	// sentinel: Command dropped (the blob owns the data now), Index and
	// Term kept.
	newLog := make([]LogEntry, len(rf.log)-(index-offset))
	copy(newLog, rf.log[index-offset:])
	newLog[0].Command = nil
	rf.log = newLog

	// state and blob in one Save(): after the trim, the blob is the only
	// copy of the burned history
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	rf.persister.Save(w.Bytes(), snapshot)
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call Kill(). goroutines use killed() to check whether
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

// field names in RPC structs must be exported, or gob drops them silently
type RequestVoteArgs struct {
	CandidateTerm int
	CandidateId   int

	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// if ReplyTerm > rf.CurrentTerm, the candidate is stale
	ReplyTerm   int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	LeaderTerm int
	LeaderId   int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	ReplyTerm int

	Success bool

	// fast backup (paper §5.3: "include the term of the conflicting
	// entry and the first index it stores for that term")
	XTerm  int // term of the conflicting entry at PrevLogIndex, -1 if log too short
	XIndex int // first index the follower stores for XTerm
	XLen   int // follower's log length, used when XTerm == -1
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            byte // unused — the lab ships snapshots whole, no chunking
	Data              []byte
	Done              bool // unused, same reason
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// stale leader, here we do not reset election timer
	if args.LeaderTerm < rf.currentTerm {
		reply.ReplyTerm = rf.currentTerm

		// figure 2 receiver rule: reply false if term < currentTerm
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

	// the log is trimmed after a snapshot, so an entry's global index
	// no longer equals its slice position. slot 0 is the sentinel and its
	// Index is the offset: slice position = global index - offset.
	offset := rf.log[0].Index

	// leader is talking about entries we already burned into the snapshot.
	// everything <= offset is committed, so it matches the leader by
	// definition — skip past the overlap instead of indexing negative slots.
	if args.PrevLogIndex < offset {
		if args.PrevLogIndex+len(args.Entries) <= offset {
			// the whole RPC is inside our snapshot: nothing new, but it matched
			reply.Success = true
			rf.persist()
			return
		}
		args.Entries = args.Entries[offset-args.PrevLogIndex:]
		args.PrevLogIndex = offset
		args.PrevLogTerm = rf.log[0].Term
	}

	if len(rf.log)-1 >= args.PrevLogIndex-offset && rf.log[args.PrevLogIndex-offset].Term == args.PrevLogTerm {
		// walk, don't truncate. RPCs arrive out of order: a stale, shorter
		// AppendEntries landing after a longer one still passes the
		// consistency check (it is a valid prefix), and a blind truncate
		// at its end would delete entries the leader may have already
		// committed. so take the entries one slot at a time and delete
		// only behind a real conflict — same index, different term
		// (figure 2's rule). same index and term means we already hold
		// that entry: touch nothing, and a stale RPC becomes a no-op.

		// we'll start from PrevLogIndex+1 (Entries[0]'s slot), one slot per entry.
		// i is a global index; every rf.log access subtracts offset
		for i := args.PrevLogIndex + 1; i <= args.PrevLogIndex+len(args.Entries); i++ {
			entryTerm := args.Entries[i-args.PrevLogIndex-1].Term

			// slot doesn't exist: our log ends here, append the rest and stop
			if i-offset >= len(rf.log) {
				rf.log = append(rf.log, args.Entries[i-args.PrevLogIndex-1:]...)
				break
			}

			// same index, same term: we already have it, touch nothing
			if rf.log[i-offset].Term == entryTerm {
				continue
			}

			// conflict: same index, different term. everything from here on
			// is a dead branch — truncate it and take the leader's entries
			rf.log = append(rf.log[:i-offset], args.Entries[i-args.PrevLogIndex-1:]...)
			break
		}

		reply.Success = true

	} else {
		reply.Success = false

		// fast backup: tell the leader enough to skip a whole term of
		// conflicting entries per RPC instead of walking back one by one
		if len(rf.log)-1 < args.PrevLogIndex-offset {
			// our log is too short to even have PrevLogIndex,
			// leader should restart from our log's end.
			// XLen is a global length: trimmed entries still count
			reply.XTerm = -1
			reply.XLen = offset + len(rf.log)
		} else {
			// we have an entry at PrevLogIndex but its term conflicts.
			// report that term and walk back to the first index we
			// store for it — everything in that run is the same dead
			// branch, so the leader can jump over all of it at once
			reply.XTerm = rf.log[args.PrevLogIndex-offset].Term
			i := args.PrevLogIndex
			// stop at offset+1: below that is the sentinel / burned entries
			for i > offset+1 && rf.log[i-1-offset].Term == reply.XTerm {
				i--
			}
			reply.XIndex = i
		}
	}

	// clamp to the last entry THIS RPC vouched for — our log may keep a longer
	// tail the leader said nothing about, and we must not commit into it
	if reply.Success && args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	}

	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	appendEntries := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return appendEntries
}

// a follower fell so far behind that the entries it needs were burned into
// the leader's snapshot — the leader can't build an AppendEntries for it
// anymore, so it sends the whole snapshot instead. install it: replace our
// log, jump our counters, persist blob + state together, and leave the blob
// as a note for the applier goroutine to forward up to the service.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// stale leader, refuse: a dead leader must not overwrite our state
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// live leader: adopt its term if newer, become follower, reset the
	// election timer — the snapshot proves a live leader exists
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.role = Follower
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	// old or duplicate snapshot (the network reorders RPCs): we are already
	// past this point, installing it would move us backward
	if args.LastIncludedIndex <= rf.commitIndex {
		rf.persist()
		return
	}

	offset := rf.log[0].Index
	if args.LastIncludedIndex <= offset+len(rf.log)-1 && rf.log[args.LastIncludedIndex-offset].Term == args.LastIncludedTerm {
		// our log extends past the snapshot and agrees with it — keep the
		// tail, those entries are still valid (same skip-don't-truncate
		// philosophy as the AppendEntries walk)
		newLog := append([]LogEntry(nil), rf.log[args.LastIncludedIndex-offset:]...)
		newLog[0].Command = nil
		rf.log = newLog
	} else {
		// our log is too short, or disagrees at that index (dead branch) —
		// the snapshot replaces it. the fresh sentinel carries exactly what
		// the next AppendEntries consistency check needs.
		rf.log = []LogEntry{{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm}}
	}

	// the service is about to receive the blob, so everything through
	// LastIncludedIndex counts as committed and applied
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	// blob + raft state must land on disk together, same reason as
	// Snapshot(): after the trim the blob is the only copy of that history
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	rf.persister.Save(w.Bytes(), args.Data)

	// note for the applier goroutine: forward this up applyCh
	rf.pendingSnapshot = args.Data
	rf.pendingSnapIndex = args.LastIncludedIndex
	rf.pendingSnapTerm = args.LastIncludedTerm
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
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

	// stale candidate: reject, and tell it the current term
	if args.CandidateTerm < rf.currentTerm {
		reply.VoteGranted = false
		reply.ReplyTerm = rf.currentTerm
		return
	}

	// election restriction (§5.4.1): grant only if the candidate's log is
	// at least as up to date as ours — higher last term wins outright,
	// equal last terms fall back to length. this is what guarantees the
	// winner's log holds every committed entry. the last slice element is
	// our last entry, but its global index is offset + len - 1, which is
	// what the candidate's LastLogIndex speaks.
	isValid := (args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[0].Index+len(rf.log)-1))
	// one ballot per term — but re-granting to the same candidate is fine
	// (our earlier reply may have been lost)
	voteAvailable := (rf.votedFor == -1 || rf.votedFor == args.CandidateId)
	if !voteAvailable || !isValid {
		reply.VoteGranted = false
		reply.ReplyTerm = rf.currentTerm
		return
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.resetElectionTimer()

	reply.ReplyTerm = rf.currentTerm

	rf.persist()
}

// send a RequestVote RPC to a server. Call() returns true if a reply
// arrived, false if the server, request, or reply was lost; it always
// returns eventually, so no extra timeout is needed around it.
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, -1, false
	}
	// the new entry takes the next free global index (snapshot offset + length)
	curIndx := rf.log[0].Index + len(rf.log)

	curLog := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   curIndx,
	}

	rf.log = append(rf.log, curLog)

	term = rf.currentTerm
	index = curIndx

	rf.persist()

	// wake the leader loop now so this entry replicates immediately.
	// non-blocking: if a kick is already pending, that's enough.
	select {
	case rf.startKick <- struct{}{}:
	default:
	}
	return index, term, isLeader
}

func (rf *Raft) ticker() {

	for !rf.killed() {
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

				offset := rf.log[0].Index
				prevIndex := rf.nextIndex[peer] - 1

				// this follower needs entries we burned into the snapshot —
				// we can't build an AppendEntries for it anymore. send the whole
				// snapshot instead; the follower installs it and the next tick
				// resumes normal AppendEntries from right after it.
				if prevIndex < offset {
					snapReq := InstallSnapshotArgs{
						Term:              term,
						LeaderId:          rf.me,
						LastIncludedIndex: offset,
						LastIncludedTerm:  rf.log[0].Term,
						Data:              rf.persister.ReadSnapshot(),
					}
					snapRes := InstallSnapshotReply{}
					rf.mu.Unlock()

					go func(p int, req InstallSnapshotArgs, res InstallSnapshotReply) {
						if rf.sendInstallSnapshot(p, &req, &res) {

							// follower has a higher term: we are a stale leader
							rf.mu.Lock()
							defer rf.mu.Unlock()

							if res.Term > rf.currentTerm {
								rf.currentTerm = res.Term
								rf.votedFor = -1
								rf.role = Follower
								rf.resetElectionTimer()
								rf.persist()
								return
							}

							// drop stale replies, same rule as AppendEntries:
							// a reply is only valid for the term it was sent in
							if rf.role != Leader || rf.currentTerm != req.Term {
								return
							}

							// the follower now has everything through
							// LastIncludedIndex. max keeps matchIndex from
							// ever moving backward on a late reply.
							rf.matchIndex[p] = max(rf.matchIndex[p], req.LastIncludedIndex)
							rf.nextIndex[p] = rf.matchIndex[p] + 1
						}
					}(peer, snapReq, snapRes)

					// this tick, this follower gets the snapshot instead of entries
					continue
				}

				appendRPCreq := AppendEntriesArgs{
					LeaderTerm:   term,
					LeaderId:     rf.me,
					PrevLogIndex: prevIndex,
					PrevLogTerm:  rf.log[prevIndex-offset].Term,
					Entries:      append([]LogEntry{}, rf.log[prevIndex+1-offset:]...),
					LeaderCommit: rf.commitIndex,
				}
				appendRPCres := AppendEntriesReply{}
				rf.mu.Unlock()

				go func(p int, req AppendEntriesArgs, res AppendEntriesReply) {
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

						// a reply is only valid for the term it was requested in:
						// if we lost and re-won leadership since this RPC went
						// out, its reply answers a previous incarnation's
						// question and must not touch this term's fresh
						// nextIndex/matchIndex bookkeeping.
						if rf.role != Leader || rf.currentTerm != req.LeaderTerm {
							return
						}

						if res.Success == false {
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
								// those match, so start right after our last one.
								// i here is a slice position — the entry's own
								// Index field gives the global index
								for i := len(rf.log) - 1; i > 0; i-- {
									if rf.log[i].Term == res.XTerm {
										rf.nextIndex[p] = rf.log[i].Index + 1
										break
									}
								}
							}
						} else {

							// max: replies arrive out of order even within one
							// term, and matchIndex must never move backward
							rf.matchIndex[p] = max(rf.matchIndex[p], req.PrevLogIndex+len(req.Entries))
							rf.nextIndex[p] = rf.matchIndex[p] + 1

							// this ack may be the one that reaches majority — advance
							// commitIndex here instead of waiting for the ticker's
							// next round. same scan as the ticker's: current-term only.
							for n := rf.log[0].Index + len(rf.log) - 1; n > rf.commitIndex; n-- {
								if rf.log[n-rf.log[0].Index].Term != rf.currentTerm {
									continue
								}
								count := 1
								for q := range rf.peers {
									if q != rf.me && rf.matchIndex[q] >= n {
										count++
									}
								}
								if count > len(rf.peers)/2 {
									rf.commitIndex = n
									break
								}
							}
						}
					}
				}(peer, appendRPCreq, appendRPCres)
			}

			// commit scan: advance commitIndex to the highest n that a
			// majority holds — counting only current-term entries (§5.4.2,
			// figure 8: an older-term entry on a majority can still be
			// overwritten; it commits indirectly when a current-term entry
			// above it does). n is a global index, so it starts at
			// offset + len - 1 and every rf.log access subtracts.
			rf.mu.Lock()
			for n := rf.log[0].Index + len(rf.log) - 1; n > rf.commitIndex; n-- {
				if rf.log[n-rf.log[0].Index].Term != rf.currentTerm {
					continue
				}

				count := 1 // our own copy
				for p := range rf.peers {
					if p == rf.me {
						continue
					}
					if rf.matchIndex[p] >= n {
						count++
					}
				}

				if count > len(rf.peers)/2 {
					rf.commitIndex = n
				}
			}
			rf.mu.Unlock()

			// sleep up to 100ms before the next round (idle heartbeats are
			// capped at 10/sec) — but wake early if Start() just appended
			// an entry, so ops don't pay the heartbeat interval
			select {
			case <-rf.startKick:
			case <-time.After(100 * time.Millisecond):
			}
			// leader round done — skip the election logic below
			continue
		}

		// pause here so the follower/candidate path doesn't busy-spin;
		// the leader path paces itself with the select at the end of its branch
		time.Sleep(10 * time.Millisecond)

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
		rf.currentTerm++
		term = rf.currentTerm
		rf.resetElectionTimer()

		// global last index; term still read from the slice's last slot
		lastLogIndx := rf.log[0].Index + len(rf.log) - 1
		lastLogTerm := rf.log[len(rf.log)-1].Term

		rf.persist()
		rf.mu.Unlock()

		// send request vote RPC
		reqRPC := RequestVoteArgs{
			CandidateTerm: term,
			CandidateId:   rf.me,

			LastLogIndex: lastLogIndx,
			LastLogTerm:  lastLogTerm,
		}
		resRPC := RequestVoteReply{}

		noPeers := len(rf.peers)
		ch := make(chan int, noPeers)
		done := make(chan struct{})

		for i := range rf.peers {
			// a valid leader's AppendEntries may have demoted us mid-fan-out;
			// a candidate that has seen a real leader stops asking for votes
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
						case <-done: // election ended, drop vote
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
				// leader bookkeeping is rebuilt on every election win:
				// nextIndex is the optimistic guess (probed downward),
				// matchIndex the pessimistic proof (acked upward)
				for i := range rf.peers {
					// next free global index, not slice length
					rf.nextIndex[i] = rf.log[0].Index + len(rf.log)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = Follower
	// each server boots with its own random 400-700ms deadline — this is what
	// breaks the boot-time synchronization (all five used to share now+400ms)
	rf.resetElectionTimer()

	rf.log = []LogEntry{{Term: 0, Index: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// buffered 1: one pending kick is enough to wake the next round
	rf.startKick = make(chan struct{}, 1)

	// boot-time defaults; a leader rebuilds these on every election win
	for p := range peers {
		rf.nextIndex[p] = len(rf.log)
		rf.matchIndex[p] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// everything through the sentinel is inside the snapshot: already
	// committed, already applied — the service restores itself from the
	// blob, so the applier must not count up from zero after a reboot
	rf.commitIndex = rf.log[0].Index
	rf.lastApplied = rf.log[0].Index

	// start ticker goroutine to start elections
	go rf.ticker()

	go func() {
		// apply data to the state machine
		for !rf.killed() {
			rf.mu.Lock()

			// a snapshot the InstallSnapshot handler left as a note goes up
			// first — it replaces everything the service had, so sending
			// commands before it would be wasted work
			if rf.pendingSnapshot != nil {
				msg := raftapi.ApplyMsg{
					SnapshotValid: true,
					Snapshot:      rf.pendingSnapshot,
					SnapshotTerm:  rf.pendingSnapTerm,
					SnapshotIndex: rf.pendingSnapIndex,
				}
				rf.pendingSnapshot = nil
				rf.mu.Unlock()

				applyCh <- msg
				continue
			}

			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				// lastApplied is a global index — subtract offset for the slice
				cmd := rf.log[rf.lastApplied-rf.log[0].Index].Command
				indx := rf.log[rf.lastApplied-rf.log[0].Index].Index
				rf.mu.Unlock()

				// applyCh is unbuffered: the send parks until the service
				// receives, which is why the lock is released first
				applyCh <- raftapi.ApplyMsg{CommandValid: true, Command: cmd, CommandIndex: indx}
			} else {
				rf.mu.Unlock()

				// nothing new to apply. without a pause this loop would re-check the
				// same thing millions of times a second and hog the CPU. nap 10ms,
				// then check again.
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()
	return rf
}
