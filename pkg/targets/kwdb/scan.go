package kwdb

import (
	"bytes"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
	"sync"
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
	ChansLen      int
}

var casetype string

func (i *indexer) GetIndex(item data.LoadedPoint) uint {
	p := item.Data.(*point)
	if p.fieldCount == 11 && p.sqlType != Insert {
		return 0
	}

	if p.fieldCount == 11 {
		targetChans := &i.node2Chan
		index := uint(targetChans.chans[targetChans.idx])
		targetChans.idx++
		if targetChans.idx == len(targetChans.chans) {
			targetChans.idx = 0
		}
		return index
	} else {
		l := len(p.device) - 1
		for l >= 0 && p.device[l] >= '0' && p.device[l] <= '9' {
			l--
		}

		var num int64
		for j := l + 1; j < len(p.device); j++ {
			num = num*10 + int64(p.device[j]-'0')
		}

		modVal := i.ChansLen / 2
		index := int(num) % modVal
		if (p.sqlType != Insert && p.template == "diagnostics") || (p.sqlType == Insert && p.fieldCount != 8) {
			index += i.numChan / 2
		}
		return uint(index)
	}
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
