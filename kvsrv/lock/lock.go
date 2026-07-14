package lock

import (
	"raft-kv/kvtest"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// lock identity/state lives in the k/v store itself
}

// MakeLock() receives a k/v clerk; lock methods
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// lock identity/state lives in the k/v store itself
	return lk
}

func (lk *Lock) Acquire() {
}

func (lk *Lock) Release() {
}
