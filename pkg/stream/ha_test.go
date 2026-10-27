package stream_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// manualClock is a clock the test moves by hand.
//
// The lease expiry is measured against it, so a test can let a lease lapse
// without waiting for it in real time.
type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualClock() *manualClock {
	return &manualClock{now: time.Unix(1700000000, 0).UTC()}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Sleep waits a moment rather than the requested duration: the clock only
// moves when the test says so, and a wait that honoured the duration would
// make every retry take seconds.
func (c *manualClock) Sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Millisecond):
		return nil
	}
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// newElection returns an election over a lease file in a temporary
// directory.
func newElection(t *testing.T, path string, clock stream.Clock) *stream.LeaderElection {
	t.Helper()
	election, err := stream.NewLeaderElection(stream.LeaderElectionOptions{
		Path:      path,
		Holder:    filepath.Base(path) + "-" + t.Name(),
		Heartbeat: 10 * time.Millisecond,
		TTL:       30 * time.Millisecond,
		Clock:     clock,
	})
	if err != nil {
		t.Fatalf("NewLeaderElection() error = %v", err)
	}
	return election
}

// TestLeaderElectionGrantsTheLeaseToOneInstance is the core guarantee: two
// instances sharing a lease file must never both believe they lead.
func TestLeaderElectionGrantsTheLeaseToOneInstance(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "prism.lock")
	clock := newManualClock()

	first := newElection(t, path, clock)
	defer func() { _ = first.Close() }()
	second := newElection(t, path, clock)
	defer func() { _ = second.Close() }()

	acquired, err := first.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if !acquired {
		t.Fatal("the first instance did not get the lease")
	}
	if !first.IsLeader() {
		t.Error("IsLeader() = false after acquiring the lease")
	}

	acquired, err = second.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if acquired {
		t.Error("the second instance got the lease while the first held it")
	}
	if second.IsLeader() {
		t.Error("IsLeader() = true for an instance that did not get the lease")
	}
}

// TestLeaderElectionFailsOverWhenTheLeaderLeaves covers the standby taking
// over once the leader releases the lease.
func TestLeaderElectionFailsOverWhenTheLeaderLeaves(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "prism.lock")
	clock := newManualClock()

	leader := newElection(t, path, clock)
	standby := newElection(t, path, clock)
	defer func() { _ = standby.Close() }()

	acquired, err := leader.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if !acquired {
		t.Fatal("the leader did not get the lease")
	}
	leaderEpoch := leader.Epoch()
	if leaderEpoch == 0 {
		t.Fatal("Epoch() = 0 for the leader, want the epoch of its term")
	}

	// The leader goes away.
	if err := leader.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if leader.IsLeader() {
		t.Error("IsLeader() = true after the lease was released")
	}

	acquired, err = standby.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if !acquired {
		t.Fatal("the standby did not take the lease over")
	}
	if standby.Epoch() <= leaderEpoch {
		t.Errorf("Epoch() = %d after the handover, want more than the previous %d",
			standby.Epoch(), leaderEpoch)
	}
}

// TestLeaderElectionFencesALapsedLease covers the case the advisory lock
// cannot: a leader that is alive but no longer renewing. Its lease lapses,
// a standby claims it with a higher epoch, and the old leader notices on its
// next renewal that it has been replaced.
func TestLeaderElectionFencesALapsedLease(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "prism.lock")
	clock := newManualClock()

	leader := newElection(t, path, clock)
	defer func() { _ = leader.Close() }()
	standby := newElection(t, path, clock)
	defer func() { _ = standby.Close() }()

	if acquired, err := leader.Acquire(context.Background()); err != nil || !acquired {
		t.Fatalf("Acquire() = %v, %v, want the lease", acquired, err)
	}
	leaderEpoch := leader.Epoch()

	// The leader stops renewing. Its lease lapses, but it still holds the
	// advisory lock because it has not noticed anything yet.
	clock.advance(time.Second)

	// The standby asks for the lease. It cannot take the lock, but it can
	// fence the lapsed lease.
	if _, err := standby.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if standby.IsLeader() {
		t.Error("the standby leads before it holds the lock")
	}

	// The old leader tries to renew and is told it has been replaced.
	renewed, err := leader.Renew(context.Background())
	if err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	if renewed {
		t.Error("Renew() = true for a leader that was fenced")
	}
	if leader.IsLeader() {
		t.Error("IsLeader() = true after the lease was fenced")
	}

	// With the lock released, the standby takes over.
	if err := leader.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	acquired, err := standby.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if !acquired {
		t.Fatal("the standby did not take the lease over")
	}
	if standby.Epoch() <= leaderEpoch {
		t.Errorf("Epoch() = %d after the takeover, want more than %d",
			standby.Epoch(), leaderEpoch)
	}
}

// TestLeaderElectionRenewExtendsTheLease checks that a leader that keeps
// renewing keeps the lease.
func TestLeaderElectionRenewExtendsTheLease(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "prism.lock")
	clock := newManualClock()

	leader := newElection(t, path, clock)
	defer func() { _ = leader.Close() }()

	if acquired, err := leader.Acquire(context.Background()); err != nil || !acquired {
		t.Fatalf("Acquire() = %v, %v, want the lease", acquired, err)
	}
	before, found := leader.Record()
	if !found {
		t.Fatal("Record() found no lease record")
	}

	clock.advance(time.Second)
	renewed, err := leader.Renew(context.Background())
	if err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	if !renewed {
		t.Fatal("Renew() = false, want the lease to be extended")
	}

	after, _ := leader.Record()
	if !after.ExpiresAt.After(before.ExpiresAt) {
		t.Errorf("the lease was not extended: expires at %v, was %v",
			after.ExpiresAt, before.ExpiresAt)
	}
}

// TestTailerStandsByWhileAnotherInstanceLeads is the property the lease
// exists for: a standby must not emit a single record, because the leader is
// already emitting it.
func TestTailerStandsByWhileAnotherInstanceLeads(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "prism.lock")
	leader := newElection(t, path, stream.RealClock{})
	defer func() { _ = leader.Close() }()
	if acquired, err := leader.Acquire(context.Background()); err != nil || !acquired {
		t.Fatalf("Acquire() = %v, %v, want the lease", acquired, err)
	}

	standby := newElection(t, path, stream.RealClock{})
	defer func() { _ = standby.Close() }()

	events := fakerpc.MakeEvents(100, 5)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(10))
	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 100, PageLimit: 10})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	sink := &collector{}
	tailer := stream.NewTailer(src, sink, stream.Options{
		Follow:     true,
		Leader:     standby,
		Clock:      stream.RealClock{},
		OnRetry:    func(int, time.Duration, error) {},
		OnProgress: func(stream.Progress) {},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	if err := tailer.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v, want a clean stop", err)
	}
	if len(sink.ids) != 0 {
		t.Errorf("the standby emitted %d records, want 0: %v", len(sink.ids), sink.ids)
	}
}

// TestTailerTakesOverWhenTheLeaderLeaves covers the handover: the standby
// waits, and streams as soon as the lease is free.
func TestTailerTakesOverWhenTheLeaderLeaves(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "prism.lock")
	leader := newElection(t, path, stream.RealClock{})
	defer func() { _ = leader.Close() }()
	if acquired, err := leader.Acquire(context.Background()); err != nil || !acquired {
		t.Fatalf("Acquire() = %v, %v, want the lease", acquired, err)
	}

	standby := newElection(t, path, stream.RealClock{})
	defer func() { _ = standby.Close() }()

	events := fakerpc.MakeEvents(100, 5)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(10))
	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 100, PageLimit: 10})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	resumed := ""
	sink := &collector{}
	tailer := stream.NewTailer(src, sink, stream.Options{
		Leader: standby,
		Clock:  stream.RealClock{},
		LeaderResume: func(context.Context) (string, error) {
			return resumed, nil
		},
	})

	done := make(chan error, 1)
	go func() {
		done <- tailer.Run(context.Background())
	}()

	// The standby is waiting for the lease; give it a moment to prove it
	// does not stream while the leader holds it.
	time.Sleep(50 * time.Millisecond)
	if len(sink.ids) != 0 {
		t.Fatalf("the standby emitted %d records while another instance led: %v",
			len(sink.ids), sink.ids)
	}

	// The leader goes away and the standby takes over.
	if err := leader.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the standby did not take over")
	}

	if len(sink.ids) == 0 {
		t.Error("the standby emitted no records after taking over")
	}
}

// TestLeaderElectionRejectsAMissingDirectory checks that a mistyped lease
// path is reported up front rather than at the first attempt to take it.
func TestLeaderElectionRejectsAMissingDirectory(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "missing", "prism.lock")
	if _, err := stream.NewLeaderElection(stream.LeaderElectionOptions{Path: path}); err == nil {
		t.Fatal("NewLeaderElection() error = nil, want a directory failure")
	}
}

// TestLeaderElectionRequiresAPath checks the empty path is refused.
func TestLeaderElectionRequiresAPath(t *testing.T) {
	t.Parallel()

	if _, err := stream.NewLeaderElection(stream.LeaderElectionOptions{}); err == nil {
		t.Fatal("NewLeaderElection() error = nil, want a path failure")
	}
}

// TestLeaderElectionReleaseIsSafeWithoutTheLease checks that releasing a
// lease that was never taken is not an error.
func TestLeaderElectionReleaseIsSafeWithoutTheLease(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "prism.lock")
	election := newElection(t, path, stream.RealClock{})
	if err := election.Release(context.Background()); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if err := election.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// TestLeaderElectionEnsureWaitsForTheLease checks that Ensure blocks rather
// than reporting leadership it does not have.
func TestLeaderElectionEnsureWaitsForTheLease(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "prism.lock")
	leader := newElection(t, path, stream.RealClock{})
	defer func() { _ = leader.Close() }()
	if acquired, err := leader.Acquire(context.Background()); err != nil || !acquired {
		t.Fatalf("Acquire() = %v, %v, want the lease", acquired, err)
	}

	standby := newElection(t, path, stream.RealClock{})
	defer func() { _ = standby.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	leading, err := standby.Ensure(ctx)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if leading {
		t.Error("Ensure() = true while another instance held the lease")
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Errorf("ctx.Err() = %v, want the deadline to have passed", ctx.Err())
	}
}
