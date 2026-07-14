package kvraft

import (
	"raft-kv/kvsrv/rpc"
	"raft-kv/kvtest"
	"raft-kv/tester"
)


type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader int // last successful leader (index into servers[])
	// You can add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.

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

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
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
