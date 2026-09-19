package baseapp

import (
	"testing"
	"time"
)

func TestABCITimingStatSnapshotAndReset(t *testing.T) {
	var stat abciTimingStat
	stat.observe(2 * time.Millisecond)
	stat.observe(5 * time.Millisecond)
	stat.observe(3 * time.Millisecond)

	snapshot := stat.snapshotAndReset()
	if snapshot.count != 3 {
		t.Fatalf("count = %d, want 3", snapshot.count)
	}
	if snapshot.total != 10*time.Millisecond {
		t.Fatalf("total = %s, want 10ms", snapshot.total)
	}
	if snapshot.avg != 3*time.Millisecond+333*time.Microsecond+333*time.Nanosecond {
		t.Fatalf("avg = %s, want 3.333333ms", snapshot.avg)
	}
	if snapshot.max != 5*time.Millisecond {
		t.Fatalf("max = %s, want 5ms", snapshot.max)
	}

	empty := stat.snapshotAndReset()
	if empty.count != 0 || empty.total != 0 || empty.avg != 0 || empty.max != 0 {
		t.Fatalf("second snapshot = %+v, want zero values", empty)
	}
}
