package kvraft

import (
	"raft-kv/kvraft/rsm"
	"raft-kv/kvsrv/rpc"
	"raft-kv/labgob"
	"raft-kv/labrpc"
	"raft-kv/tester"
)

type entry struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// no mutex: DoOp is the only code that touches kvs, and it is only
	// ever called by the rsm's single reader goroutine, one op at a time
	kvs map[string]entry
}

// DoOp executes one committed op against the store. it runs only on the
// rsm's reader goroutine, in log order, identically on every server.
func (kv *KVServer) DoOp(req any) any {
	switch args := req.(type) {
	case rpc.GetArgs:
		e, ok := kv.kvs[args.Key]
		if !ok {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{Value: e.Value, Version: e.Version, Err: rpc.OK}

	case rpc.PutArgs:
		e, exists := kv.kvs[args.Key]

		// rule 1: new key — version 0 means "create it"
		if !exists && args.Version == 0 {
			kv.kvs[args.Key] = entry{Value: args.Value, Version: 1}
			return rpc.PutReply{Err: rpc.OK}
		}

		// rule 2: key doesn't exist and caller thought it did
		if !exists {
			return rpc.PutReply{Err: rpc.ErrNoKey}
		}

		// rule 3: key changed since the caller last read it
		if args.Version != e.Version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}

		// rule 4: version matches — do the write, bump the version
		kv.kvs[args.Key] = entry{Value: args.Value, Version: e.Version + 1}
		return rpc.PutReply{Err: rpc.OK}
	}
	return nil
}

// kv-layer snapshots (lab 4C) are not implemented; Snapshot and Restore
// exist to satisfy the rsm.StateMachine interface.
func (kv *KVServer) Snapshot() []byte {
	return nil
}

func (kv *KVServer) Restore(data []byte) {
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// submit the value (not the pointer): labgob registered rpc.GetArgs,
	// and the value form is what comes back out of the log into DoOp
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.PutReply)
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// labgob must know every concrete type that travels through the log
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	// the map must exist before MakeRSM: MakeRSM starts the reader
	// goroutine, which can call DoOp immediately
	kv := &KVServer{me: me, kvs: make(map[string]entry)}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
