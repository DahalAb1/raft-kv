// Package kvsrv is the single-server ancestor of kvraft: it exists in
// this repo to define the versioned Put/Get API (see rpc/) that the
// replicated service preserves. The client and server here are interface
// stubs — the single-server implementation is omitted per course policy;
// the implemented, replicated versions live in kvraft/.
package kvsrv

import (
	"raft-kv/kvsrv/rpc"
	"raft-kv/kvtest"
	"raft-kv/tester"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	return ck
}

// Get fetches the current value and version for a key. It returns
// ErrNoKey if the key does not exist.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// stub — see kvraft/client.go for the implemented, replicated Clerk
	return "", 0, rpc.ErrNoKey
}

// Put updates key with value only if the version in the request matches
// the version of the key at the server; mismatches return ErrVersion,
// and an ErrVersion on a resend is reported as ErrMaybe. See
// kvraft/client.go for the full contract.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// stub — see kvraft/client.go for the implemented, replicated Clerk
	return rpc.ErrNoKey
}
