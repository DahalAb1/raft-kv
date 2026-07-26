package rsm

import (
	"sync"
	"time"

	"raft-kv/kvsrv/rpc"
	"raft-kv/labrpc"
	"raft-kv/raft"
	"raft-kv/raftapi"
	"raft-kv/tester"
)

type Op struct {
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int
	Req interface{}
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	nextId       int
	waiters      map[int]chan any // log index → pager of the Submit parked at that index
}

// MakeRSM wires a state machine to a Raft instance. me is this server's
// index in servers[]; maxraftstate is the snapshot threshold (-1 disables
// snapshotting). MakeRSM must return quickly, so the long-running work
// lives in the reader goroutine it starts.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		waiters:      make(map[int]chan any),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	// one reader goroutine for the server's whole life: drain every committed
	// op off applyCh in log order and apply it to the state machine. if a
	// Submit is waiting at that index, hand it the result and wake it up.
	go func() {
		for {
			msg := <-rsm.applyCh
			if !msg.CommandValid {
				continue
			}
			op := msg.Command.(Op)
			res := rsm.sm.DoOp(op.Req)

			rsm.mu.Lock()

			ch, ok := rsm.waiters[msg.CommandIndex]
			if ok {
				delete(rsm.waiters, msg.CommandIndex)
			}
			rsm.mu.Unlock()

			if ok {
				// two sends: first the op that actually committed (so the
				// waiter can check it is really its own, not an impostor
				// from a newer leader), then the result DoOp produced
				ch <- msg.Command
				ch <- res
			}
		}
	}()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	rsm.mu.Lock()
	rsm.nextId += 1
	cmd := Op{Me: rsm.me, Id: rsm.nextId, Req: req}

	// Start and the waiter registration happen under the same lock so the
	// reader can't process the commit before the waiter is on the board.
	// Start's term is kept as the reference point for noticing leadership
	// changes while we wait.
	index, startTerm, isLeader := rsm.rf.Start(cmd)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	// buffered so the reader's two sends always land even if we've walked
	// away on timeout — an unbuffered send to an abandoned pager would
	// freeze the applier, which stalls applyCh, which stalls Raft itself.
	ch := make(chan any, 2)
	rsm.waiters[index] = ch
	rsm.mu.Unlock()

	// the wait: wake on either a commit at our index, or a periodic check
	// that leadership still holds. "my op will never commit" arrives as
	// silence — no commit at this index at all — so the pager alone can't
	// deliver that answer; we have to ask Raft ourselves.
	for {
		select {
		case v := <-ch:
			op := v.(Op)
			res := <-ch

			// impostor check: leadership may have changed after Start, and a new
			// leader's op can commit at the index we were promised. ids only
			// increase per server, so a mismatch means our op was overwritten and
			// will never commit — the client must retry elsewhere.
			if op.Me != rsm.me || op.Id != cmd.Id {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, res

		// no commit news: every 100ms, re-check that the regime that made
		// our promise still holds. the tick is a heartbeat, not a deadline —
		// nothing gives up because time passed, only because the answer changed
		case <-time.After(100 * time.Millisecond):
			term, isLeader := rsm.rf.GetState()
			if term == startTerm && isLeader {
				continue // still in charge, promise still good, keep waiting
			}
			// leadership gone — but only give up if our pager is still on the
			// board. if the reader already took it, a result is en route and
			// the next loop iteration will receive it; returning ErrWrongLeader
			// for an op that actually committed would make the client re-run it.
			rsm.mu.Lock()
			_, waiting := rsm.waiters[index]
			if waiting {
				delete(rsm.waiters, index)
			}
			rsm.mu.Unlock()
			if waiting {
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}
