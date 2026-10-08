package kwdb

import (
	"fmt"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/timescale/tsbs/pkg/targets"
)

const maxWriteLatency = time.Hour

type writeLatencyStats struct {
	recorder *writeLatencyRecorder
}

type writeLatencyRecorder struct {
	mu          sync.Mutex
	histogram   *hdrhistogram.Histogram
	workerCount int
	stats       writeLatencyStats
}

func newWriteLatencyHistogram() *hdrhistogram.Histogram {
	return hdrhistogram.New(1, maxWriteLatency.Microseconds(), 3)
}

func newWriteLatencyRecorder(workerCount int) *writeLatencyRecorder {
	recorder := &writeLatencyRecorder{
		histogram:   newWriteLatencyHistogram(),
		workerCount: workerCount,
	}
	recorder.stats.recorder = recorder
	return recorder
}

func (r *writeLatencyRecorder) worker(workerNum int) *writeLatencyStats {
	if r == nil || workerNum < 0 || workerNum >= r.workerCount {
		return nil
	}
	return &r.stats
}

func (s *writeLatencyStats) record(elapsed time.Duration) {
	value := elapsed.Microseconds()
	if value < 1 {
		value = 1
	} else if value > maxWriteLatency.Microseconds() {
		value = maxWriteLatency.Microseconds()
	}

	s.recorder.mu.Lock()
	_ = s.recorder.histogram.RecordValue(value)
	s.recorder.mu.Unlock()
}

func (s *writeLatencyStats) finish(start time.Time) {
	s.record(time.Since(start))
}

func (r *writeLatencyRecorder) print() {
	// Reporting happens after all loader workers have stopped.
	count := r.histogram.TotalCount()
	if count == 0 {
		fmt.Println("\nWrite request latency: no data write requests recorded")
		return
	}

	toMillis := func(value float64) float64 {
		return value / float64(time.Millisecond/time.Microsecond)
	}
	fmt.Println("\nWrite request latency (all workers):")
	fmt.Printf("mean: %.2fms, p50: %.2fms, p90: %.2fms, p95: %.2fms, p99: %.2fms, max: %.2fms, count: %d\n",
		toMillis(r.histogram.Mean()),
		toMillis(float64(r.histogram.ValueAtQuantile(50))),
		toMillis(float64(r.histogram.ValueAtQuantile(90))),
		toMillis(float64(r.histogram.ValueAtQuantile(95))),
		toMillis(float64(r.histogram.ValueAtQuantile(99))),
		toMillis(float64(r.histogram.Max())),
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
