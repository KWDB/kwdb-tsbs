package kwdb

import (
	"fmt"
	"io"
	"math/bits"
	"os"
	"time"

	"github.com/timescale/tsbs/pkg/targets"
)

const (
	latencyExactBuckets    = 64
	latencyBucketsPerPower = 64
	latencyFirstPower      = 6
	latencyLastPower       = 41
	latencyBucketCount     = latencyExactBuckets + (latencyLastPower-latencyFirstPower+1)*latencyBucketsPerPower
	maxWriteLatency        = time.Hour
)

type writeLatencyStats struct {
	// Each successful request contributes one sample: request duration / rows.
	// Keep the sum in nanoseconds as a float to preserve fractional row times.
	total   float64
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

func (s *writeLatencyStats) record(elapsed time.Duration, rows int) {
	if rows <= 0 {
		return
	}
	if elapsed < 0 {
		elapsed = 0
	}
	perRow := float64(elapsed) / float64(rows)
	s.total += perRow
	s.count++

	nanos := uint64(perRow)
	if nanos > uint64(maxWriteLatency) {
		nanos = uint64(maxWriteLatency)
	}
	s.buckets[latencyBucketIndex(nanos)]++
}

func latencyBucketIndex(nanos uint64) int {
	if nanos < latencyExactBuckets {
		return int(nanos)
	}
	power := bits.Len64(nanos) - 1
	base := uint64(1) << power
	offset := int((nanos - base) * latencyBucketsPerPower / base)
	return latencyExactBuckets + (power-latencyFirstPower)*latencyBucketsPerPower + offset
}

func latencyBucketUpperBound(index int) int64 {
	if index < latencyExactBuckets {
		return int64(index)
	}
	index -= latencyExactBuckets
	power := latencyFirstPower + index/latencyBucketsPerPower
	offset := index % latencyBucketsPerPower
	base := int64(1) << power
	return base + int64(offset+1)*base/latencyBucketsPerPower - 1
}

// valueAtQuantile takes a quantile in thousandths: 500 = P50, 999 = P99.9.
// Bucket values have nanosecond resolution and at most about 1.6% bucket error.
func (r *writeLatencyRecorder) valueAtQuantile(quantile uint64, count uint64) int64 {
	if count == 0 {
		return 0
	}
	// Calculate ceil(count * quantile / 1000) without overflowing the product.
	target := count/1000*quantile + (count%1000*quantile+999)/1000
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
	r.writeSummary(os.Stdout)
}

func (r *writeLatencyRecorder) writeSummary(w io.Writer) {
	var total float64
	var count uint64
	for i := range r.workers {
		worker := &r.workers[i]
		total += worker.total
		count += worker.count
	}

	if count == 0 {
		fmt.Fprintln(w, "\nWrite latency per row: no successful data write requests recorded")
		return
	}

	fmt.Fprintln(w, "\nWrite latency per row (all workers):")
	fmt.Fprintf(w, "mean: %.3fms/row, p50: %.3fms/row, p99: %.3fms/row, p99.9: %.3fms/row\n",
		total/float64(count)/float64(time.Millisecond),
		float64(r.valueAtQuantile(500, count))/float64(time.Millisecond),
		float64(r.valueAtQuantile(990, count))/float64(time.Millisecond),
		float64(r.valueAtQuantile(999, count))/float64(time.Millisecond),
	)
}

// ReportWriteLatency prints per-row latency statistics for successful writes
// across all KWDB loader workers. It has no effect when collection is disabled.
func ReportWriteLatency(b targets.Benchmark) {
	benchmark, ok := b.(*benchmark)
	if !ok || benchmark.writeLatencyRecorder == nil {
		return
	}
	benchmark.writeLatencyRecorder.print()
}
