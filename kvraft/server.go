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

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
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
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	// the map must exist before MakeRSM: MakeRSM starts the reader
	// goroutine, which can call DoOp immediately
	kv := &KVServer{me: me, kvs: make(map[string]entry)}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
