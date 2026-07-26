package kvsrv

import (
	"log"
	"sync"

	"raft-kv/kvsrv/rpc"
	"raft-kv/labrpc"
	"raft-kv/tester"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// stub — see kvraft/server.go for the implemented, replicated server
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// stub — see kvraft/server.go for the implemented, replicated server
}

// the arguments exist to match the replicated servers' constructor
// signature; the single-server version has no use for them
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
