package kwdb

import (
	"fmt"
	"time"

	"github.com/timescale/tsbs/pkg/targets"
)

type writeLatencyStats struct {
	total time.Duration
	max   time.Duration
	count uint64

	// Keep counters written by different workers on separate cache lines. The
	// extra cache line also makes this safe when the slice base is not aligned
	// to a cache-line boundary.
	_ [104]byte
}

type writeLatencyRecorder struct {
	workers []writeLatencyStats
}

func newWriteLatencyRecorder(workerCount int) *writeLatencyRecorder {
	if workerCount < 0 {
		workerCount = 0
	}
	return &writeLatencyRecorder{workers: make([]writeLatencyStats, workerCount)}
}

func (r *writeLatencyRecorder) worker(workerNum int) *writeLatencyStats {
	if r == nil || workerNum < 0 || workerNum >= len(r.workers) {
		return nil
	}
	return &r.workers[workerNum]
}

func (s *writeLatencyStats) record(elapsed time.Duration) {
	if elapsed < 0 {
		elapsed = 0
	}
	s.total += elapsed
	s.count++
	if elapsed > s.max {
		s.max = elapsed
	}
}

func (s *writeLatencyStats) finish(start time.Time) {
	s.record(time.Since(start))
}

func (r *writeLatencyRecorder) print() {
	var total, max time.Duration
	var count uint64
	for i := range r.workers {
		worker := &r.workers[i]
		total += worker.total
		count += worker.count
		if worker.max > max {
			max = worker.max
		}
	}

	if count == 0 {
		fmt.Println("\nWrite request latency: no data write requests recorded")
		return
	}

	fmt.Println("\nWrite request latency (all workers):")
	fmt.Printf("mean: %.2fms, max: %.2fms, count: %d\n",
		float64(total)/float64(count)/float64(time.Millisecond),
		float64(max)/float64(time.Millisecond),
		count,
	)
}

// ReportWriteLatency prints the combined latency distribution for all KWDB
// loader workers. It has no effect when write latency collection is disabled.
func ReportWriteLatency(b targets.Benchmark) {
	benchmark, ok := b.(*benchmark)
	if !ok || benchmark.writeLatencyRecorder == nil {
		return
	}
	benchmark.writeLatencyRecorder.print()
}
