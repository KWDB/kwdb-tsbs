package kwdb

import (
	"bytes"
	"sync"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
)

type Node2chan struct {
	chans []int
	idx   int
}

// indexer is used to consistently send the same hostnames to the same worker
type indexer struct {
	buffer        *bytes.Buffer
	prefix        []byte
	partitions    int
	hashEndGroups []uint32
	tmp           map[string]uint
	numChan       int
	node2Chan     Node2chan
	Nodes         int
}

func (i *indexer) GetIndex(item data.LoadedPoint) uint {
	p := item.Data.(*point)

	if p.sqlType != Insert {
		return 0
	}
	targetChans := &i.node2Chan
	index := uint(targetChans.chans[targetChans.idx])
	targetChans.idx++
	if targetChans.idx == len(targetChans.chans) {
		targetChans.idx = 0
	}
	return index
}

// point is a single row of data keyed by which superTable it belongs
type point struct {
	sqlType    byte
	template   string
	device     string
	tag        string
	fieldCount int
	sql        string
}

var GlobalTable = sync.Map{}

type hypertableArr struct {
	createSql   []*point
	m           map[string][]string
	totalMetric uint64
	cnt         uint
}

func (ha *hypertableArr) Len() uint {
	return ha.cnt
}

func (ha *hypertableArr) Append(item data.LoadedPoint) {
	that := item.Data.(*point)
	if that.sqlType == Insert {
		ha.m[that.device] = append(ha.m[that.device], that.sql)
		ha.totalMetric += uint64(that.fieldCount)
		ha.cnt++
	} else {
		ha.createSql = append(ha.createSql, that)
	}
}

func (ha *hypertableArr) Reset() {
	ha.m = map[string][]string{}
	ha.cnt = 0
	ha.createSql = ha.createSql[:0]
}

type factory struct{}

func (f *factory) New() targets.Batch {
	return &hypertableArr{
		m:   map[string][]string{},
		cnt: 0,
	}
}
