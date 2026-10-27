//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package stream

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// lockFile takes an advisory lock on f.
//
// An exclusive lock is held for the whole of a leader's term; a shared lock
// is only ever used for the short critical sections that read or write the
// lease record. The locks are advisory: they stop other prism instances
// from taking the same lease, and nothing else.
//
// When blocking is false the call fails immediately with ErrLeaseHeld when
// the lock is already held, which is what lets a standby ask for the lease
// without waiting on it.
func lockFile(f *os.File, exclusive, blocking bool) error {
	how := syscall.LOCK_SH
	if exclusive {
		how = syscall.LOCK_EX
	}
	if !blocking {
		how |= syscall.LOCK_NB
	}
	if err := syscall.Flock(int(f.Fd()), how); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return ErrLeaseHeld
		}
		return fmt.Errorf("locking %s: %w", f.Name(), err)
	}
	return nil
}

// unlockFile releases the advisory lock on f.
func unlockFile(f *os.File) error {
	if f == nil {
		return nil
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
		return fmt.Errorf("unlocking %s: %w", f.Name(), err)
	}
	return nil
}
