package watch

import (
	"context"
	"testing"
	"time"
)

func TestSilentForResetsOnTouch(t *testing.T) {
	w := &Watcher{lastOK: time.Now().Add(-time.Hour)}
	if got := w.silentFor(); got < 59*time.Minute {
		t.Fatalf("silence not measured from lastOK: %v", got)
	}
	w.touch()
	if got := w.silentFor(); got > time.Second {
		t.Fatalf("touch did not reset the clock: %v", got)
	}
}

func TestDeadAfterIsSeveralTicks(t *testing.T) {
	// the threshold must outlast one missed introspection tick by a wide
	// margin, or a single slow DescribeGroups would tear the session down
	if DeadAfter < 3*time.Minute {
		t.Fatalf("DeadAfter %v is under three default ticks", DeadAfter)
	}
}

func TestRunOnNilWatcherReturnsNil(t *testing.T) {
	var w *Watcher
	if err := w.Run(context.Background(), time.Minute); err != nil {
		t.Fatalf("nil watcher returned %v", err)
	}
}
