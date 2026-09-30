package kwdb

import (
	"fmt"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/timescale/tsbs/pkg/targets"
)

const maxWriteLatency = time.Hour

type writeLatencyStats struct {
	// Each instance is owned by exactly one loader worker, so recording does
	// not require synchronization.
	histogram *hdrhistogram.Histogram
}

type writeLatencyRecorder struct {
	workers []*writeLatencyStats
}

func newWriteLatencyHistogram() *hdrhistogram.Histogram {
	return hdrhistogram.New(1, maxWriteLatency.Microseconds(), 3)
}

func newWriteLatencyRecorder(workerCount int) *writeLatencyRecorder {
	return &writeLatencyRecorder{
		workers: make([]*writeLatencyStats, workerCount),
	}
}

func (r *writeLatencyRecorder) worker(workerNum int) *writeLatencyStats {
	if r == nil || workerNum < 0 || workerNum >= len(r.workers) {
		return nil
	}

	// Each worker initializes its own histogram after RunBenchmark has started
	// timing. Keeping the large histogram allocation out of benchmark setup
	// prevents it from changing the Go GC heap goal before the timed load.
	stats := &writeLatencyStats{
		histogram: newWriteLatencyHistogram(),
	}
	r.workers[workerNum] = stats
	return stats
}

func (s *writeLatencyStats) record(elapsed time.Duration) {
	value := elapsed.Microseconds()
	if value < 1 {
		value = 1
	} else if value > maxWriteLatency.Microseconds() {
		value = maxWriteLatency.Microseconds()
	}

	_ = s.histogram.RecordValue(value)
}

func (s *writeLatencyStats) start() time.Time {
	if s == nil {
		return time.Time{}
	}
	return time.Now()
}

func (s *writeLatencyStats) finish(start time.Time) {
	if s == nil {
		return
	}
	s.record(time.Since(start))
}

func (r *writeLatencyRecorder) print() {
	histogram := newWriteLatencyHistogram()
	for _, worker := range r.workers {
		if worker == nil {
			continue
		}
		// All workers have stopped before reporting, and every histogram uses
		// the same range, so merging is safe and cannot drop values.
		histogram.Merge(worker.histogram)
	}

	count := histogram.TotalCount()
	if count == 0 {
		fmt.Println("\nWrite request latency: no data write requests recorded")
		return
	}

	toMillis := func(value float64) float64 {
		return value / float64(time.Millisecond/time.Microsecond)
	}
	fmt.Println("\nWrite request latency (all workers):")
	fmt.Printf("mean: %.2fms, p50: %.2fms, p90: %.2fms, p95: %.2fms, p99: %.2fms, max: %.2fms, count: %d\n",
		toMillis(histogram.Mean()),
		toMillis(float64(histogram.ValueAtQuantile(50))),
		toMillis(float64(histogram.ValueAtQuantile(90))),
		toMillis(float64(histogram.ValueAtQuantile(95))),
		toMillis(float64(histogram.ValueAtQuantile(99))),
		toMillis(float64(histogram.Max())),
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
