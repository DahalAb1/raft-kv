package kvraft

import (
	"raft-kv/kvsrv/rpc"
	"raft-kv/kvtest"
	"raft-kv/tester"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get fetches the current value and version for a key. It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}

	i := ck.leader
	for {
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)

		// a real answer only comes from the leader; anything else
		// (no reply, or "I'm not the leader") means try the next server
		if ok && reply.Err != rpc.ErrWrongLeader {
			ck.leader = i
			return reply.Value, reply.Version, reply.Err
		}

		i = i + 1
		if i == len(ck.servers) {
			i = 0
		}
	}
}

// Put updates key with value only if the version in the request matches
// the version of the key at the server. If the version numbers don't
// match, the server returns ErrVersion. An ErrVersion on the first RPC
// means the Put definitely did not execute, so Put returns ErrVersion.
// An ErrVersion on a resend is reported as ErrMaybe instead: the earlier
// attempt may have executed and had its reply lost, so the Clerk cannot
// know whether the Put happened.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}

	// resent tracks whether an earlier attempt might have reached the
	// server. it flips to true the moment one RPC goes unanswered.
	resent := false

	i := ck.leader
	for {
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)

		if ok && reply.Err != rpc.ErrWrongLeader {
			ck.leader = i

			// ErrVersion on a resend is ambiguous: the lost earlier RPC
			// may have done the Put (bumping the version), and this retry
			// is seeing the bump. the server can't tell us which world
			// we're in, so report ErrMaybe and let the application decide.
			if reply.Err == rpc.ErrVersion && resent {
				return rpc.ErrMaybe
			}
			return reply.Err
		}

		// no verdict this attempt — and ErrWrongLeader is not a safe "no":
		// the server's Submit can time out on an entry already replicated
		// to a majority, which the next leader must have and will commit.
		// either way a later ErrVersion can't be trusted as a definite no.
		resent = true

		i = i + 1
		if i == len(ck.servers) {
			i = 0
		}
	}
}
