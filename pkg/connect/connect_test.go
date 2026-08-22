package connect

import (
	"context"
	"errors"
	"testing"
	"time"
)

// a dial that fails a fixed number of times and then succeeds
func failThenOK(failures int) Dial {
	n := 0
	return func(context.Context) error {
		n++
		if n <= failures {
			return errors.New("no broker")
		}
		return nil
	}
}

// pin the jitter draw to the ceiling minus one nanosecond so the sleep
// schedule is deterministic and the doubling is observable
func maxDraw(n int64) int64 { return n - 1 }

func TestLoopRetriesUntilSuccess(t *testing.T) {
	var seen []Attempt
	p := Policy{Base: time.Millisecond, Cap: 4 * time.Millisecond, Rand: maxDraw}
	err := Loop(context.Background(), p, failThenOK(3), func(a Attempt) { seen = append(seen, a) })
	if err != nil {
		t.Fatalf("loop returned %v on an eventual success", err)
	}
	if len(seen) != 4 {
		t.Fatalf("expected 4 attempts (3 failures, 1 success), got %d", len(seen))
	}
	for i := 0; i < 3; i++ {
		if seen[i].Err == nil || seen[i].N != i+1 {
			t.Fatalf("attempt %d not reported as a failure: %+v", i+1, seen[i])
		}
	}
	if seen[3].Err != nil || seen[3].Next != 0 {
		t.Fatalf("success reported with an error or a next wait: %+v", seen[3])
	}
	// the ceiling doubles from Base and stops at Cap; with maxDraw each
	// wait is ceiling-1ns: 1ms, 2ms, 4ms (cap)
	want := []time.Duration{time.Millisecond, 2 * time.Millisecond, 4 * time.Millisecond}
	for i, w := range want {
		if seen[i].Next != w-1 {
			t.Fatalf("attempt %d waited %v, want %v", i+1, seen[i].Next, w-1)
		}
	}
}

func TestLoopStopsOnContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	p := Policy{Base: time.Hour, Cap: time.Hour, Rand: maxDraw}
	attempts := 0
	err := Loop(ctx, p, func(context.Context) error { attempts++; return errors.New("down") },
		nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected the context error, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected exactly one attempt before the hour-long wait was cut, got %d", attempts)
	}
}

func TestLoopNeverReturnsADialError(t *testing.T) {
	// a dial that only ever fails must keep going until the context ends;
	// the return value is the context's, never the dial's
	ctx, cancel := context.WithCancel(context.Background())
	p := Policy{Base: time.Microsecond, Cap: time.Microsecond, Rand: maxDraw}
	count := 0
	err := Loop(ctx, p, func(context.Context) error {
		count++
		if count == 50 {
			cancel()
		}
		return errors.New("still down")
	}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled after cancelling mid-loop, got %v", err)
	}
	if count < 50 {
		t.Fatalf("loop gave up after %d attempts", count)
	}
}

func TestSleepForIsFullJitter(t *testing.T) {
	// the draw is uniform in [0, ceiling): the floor is zero and the
	// ceiling is excluded
	if got := sleepFor(time.Second, func(int64) int64 { return 0 }); got != 0 {
		t.Fatalf("floor is not zero: %v", got)
	}
	if got := sleepFor(time.Second, maxDraw); got != time.Second-1 {
		t.Fatalf("ceiling not respected: %v", got)
	}
	if got := sleepFor(0, maxDraw); got != 0 {
		t.Fatalf("zero ceiling must not draw: %v", got)
	}
}

func TestDefaultsFillZeroPolicy(t *testing.T) {
	// a zero Policy must not spin: Base and Cap take the defaults
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	var waits []time.Duration
	_ = Loop(ctx, Policy{Rand: maxDraw}, func(context.Context) error { return errors.New("down") },
		func(a Attempt) { waits = append(waits, a.Next) })
	if len(waits) == 0 || waits[0] != Defaults.Base-1 {
		t.Fatalf("zero policy did not take the default base: %v", waits)
	}
}
