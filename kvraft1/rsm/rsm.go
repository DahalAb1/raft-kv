package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"

)

type Op struct {
	// Your definitions here.
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
	// Your definitions here.
	nextId  int
	waiters map[int]chan any // waiters watis for committed data
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
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

			// map lookups have two value form built into the language
			// therefore we get okay when we just send ch
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

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
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
			op := v.(Op) // (<-)ch --> recieves data from channel gets info as any type, .(Op) --> convert any of channel's return to Op
			res := <-ch

			// impostor check: leadership may have changed after Start, and a new
			// leader's op can commit at the index we were promised. ids only
			// increase per server, so a mismatch means our op was overwritten and
			// will never commit — the client must retry elsewhere.
			if op.Me != rsm.me || op.Id != cmd.Id {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, res

			// every 100 ms we check if the leadership has changed
			// if it has then we drop the channel and clear out the waiting dic with channel's info
			// if not the above case handles it. 
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
