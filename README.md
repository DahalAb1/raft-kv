# raft-kv

Raft consensus and a fault-tolerant, linearizable key/value store in Go —
leader election, log replication, persistence, log-compacting snapshots, and a
replicated-state-machine layer driving a versioned KV service.

> Implementation built for MIT 6.5840; the course-provided test harness, RPC
> framework, and tests are not included per course policy. This repo is for
> reading; the implementation files are my own work. Because the harness
> packages (`labrpc`, `labgob`, `tester`, `kvtest`) are excluded, the repo
> intentionally does not build standalone. A small self-contained driver is
> planned.

## Layout

- `raft/` — Raft core: election, replication, persistence, snapshots
- `raftapi/` — the interface contract between Raft and the services above it
- `kvraft/` — linearizable KV service; `kvraft/rsm/` is the
  replicated-state-machine bridge between the service and Raft
- `kvsrv/` — earlier single-server versioned KV (the API `kvraft` preserves
  under replication)
- `*_Notes.md` — development notes kept while building; the commit history is
  the real debugging timeline

A full chaptered writeup (election → replication → persistence → snapshots →
rsm → KV linearizability) is in progress.
