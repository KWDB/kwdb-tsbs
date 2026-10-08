package kwdb

import (
	"fmt"
	"math/bits"
	"time"

	"github.com/timescale/tsbs/pkg/targets"
)

const (
	latencyExactBuckets    = 64
	latencyBucketsPerPower = 64
	latencyFirstPower      = 6
	latencyLastPower       = 31
	latencyBucketCount     = latencyExactBuckets + (latencyLastPower-latencyFirstPower+1)*latencyBucketsPerPower
	maxWriteLatencyMicros  = int64(time.Hour / time.Microsecond)
)

type writeLatencyStats struct {
	total   time.Duration
	max     time.Duration
	count   uint64
	buckets [latencyBucketCount]uint64

	// Prevent the last buckets of one worker from sharing a cache line with
	// the counters of the next worker.
	_ [128]byte
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

	micros := elapsed.Microseconds()
	if micros < 1 {
		micros = 1
	} else if micros > maxWriteLatencyMicros {
		micros = maxWriteLatencyMicros
	}
	s.buckets[latencyBucketIndex(uint64(micros))]++
}

func (s *writeLatencyStats) finish(start time.Time) {
	s.record(time.Since(start))
}

func latencyBucketIndex(micros uint64) int {
	if micros <= latencyExactBuckets {
		return int(micros - 1)
	}
	power := bits.Len64(micros) - 1
	base := uint64(1) << power
	offset := int((micros - base) * latencyBucketsPerPower / base)
	return latencyExactBuckets + (power-latencyFirstPower)*latencyBucketsPerPower + offset
}

func latencyBucketUpperBound(index int) int64 {
	if index < latencyExactBuckets {
		return int64(index + 1)
	}
	index -= latencyExactBuckets
	power := latencyFirstPower + index/latencyBucketsPerPower
	offset := index % latencyBucketsPerPower
	base := int64(1) << power
	return base + int64(offset+1)*base/latencyBucketsPerPower - 1
}

func (r *writeLatencyRecorder) valueAtQuantile(quantile uint64, count uint64) int64 {
	target := (count*quantile + 99) / 100
	var seen uint64
	for bucket := 0; bucket < latencyBucketCount; bucket++ {
		for worker := range r.workers {
			seen += r.workers[worker].buckets[bucket]
		}
		if seen >= target {
			return latencyBucketUpperBound(bucket)
		}
	}
	return 0
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
	fmt.Printf("mean: %.2fms, p50: %.2fms, p90: %.2fms, p95: %.2fms, p99: %.2fms, max: %.2fms, count: %d\n",
		float64(total)/float64(count)/float64(time.Millisecond),
		float64(r.valueAtQuantile(50, count))/float64(time.Millisecond/time.Microsecond),
		float64(r.valueAtQuantile(90, count))/float64(time.Millisecond/time.Microsecond),
		float64(r.valueAtQuantile(95, count))/float64(time.Millisecond/time.Microsecond),
		float64(r.valueAtQuantile(99, count))/float64(time.Millisecond/time.Microsecond),
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
