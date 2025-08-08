package devops

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"math"
	"math/rand"
	"time"
)

// A CPUOnlySimulator generates data similar to telemetry from Telegraf for only CPU metrics.
// It fulfills the Simulator interface.
type CPUOnlySimulator struct {
	*commonDevopsSimulator
	currentTime time.Time
}

// Fields returns a map of subsystems to metrics collected
func (d *CPUOnlySimulator) Fields() map[string][]string {
	return d.fields(d.hosts[0].SimulatedMeasurements[:1])
}

func (d *CPUOnlySimulator) Headers() *common.GeneratedDataHeaders {
	return &common.GeneratedDataHeaders{
		TagTypes:  d.TagTypes(),
		TagKeys:   d.TagKeys(),
		FieldKeys: d.Fields(),
	}
}

// Next advances a Point to the next state in the generator.
func (d *CPUOnlySimulator) Next(p *data.Point) bool {
	if d.emptyingQueue {
		if len(d.pointQueue) > 0 {
			*p = *d.pointQueue[0]
			d.pointQueue = d.pointQueue[1:]
			return true
		} else {
			d.emptyingQueue = false
			d.queueCounter = 0
		}
	}

	updateHostIndexAndTime(d) // 检查是否需要切换到下一组指标
	if !d.populatePoint(p, 0) {
		return false
	}

	d.pointQueue = append(d.pointQueue, copyPoint(p)) // 加入队列
	d.queueCounter++
	// 检查是否需要处理乱序
	if d.queueCounter >= d.queueSize && !d.emptyingQueue {
		d.processOutOfOrder()
		d.emptyingQueue = true
		*p = *d.pointQueue[0]
		d.pointQueue = d.pointQueue[1:]
		return true
	}
	return false
}

func (d *CPUOnlySimulator) processOutOfOrder() {
	positionGroups := make([][]*data.Point, d.Orderquantity) // 将n个点按位置分成d.Orderquantity组
	for i := 0; i < d.Orderquantity; i++ {
		positionGroups[i] = make([]*data.Point, 0, d.queueSize/d.Orderquantity)
	}

	for _, point := range d.pointQueue { // 将点分配到各自的序号组
		n := point.Hostnumber % d.Orderquantity
		positionGroups[n] = append(positionGroups[n], point)
	}

	for i := 0; i < d.Orderquantity; i++ { // 对每个序号组进行部分交换
		group := positionGroups[i]
		groupSize := len(group)
		if groupSize == 0 {
			continue
		}

		numToMove := int(math.Round(float64(d.OutOfOrder) * float64(groupSize))) // 计算该组需要移动的点数（基于总比例）

		for j := 0; j < numToMove; j++ {
			rand.Seed(time.Now().UnixNano() + int64(j)) // 添加j避免重复
			pos := rand.Intn(groupSize)
			point := group[pos]
			// 从组中移除该点
			group = append(group[:pos], group[pos+1:]...)
			groupSize--
			insertPos := rand.Intn(groupSize)
			for insertPos == pos && groupSize > 1 { // 确保插入位置不是原位置
				insertPos = rand.Intn(groupSize)
			}
			group = append(group[:insertPos], append([]*data.Point{point}, group[insertPos:]...)...) // 插入到新位置
			groupSize++
		}
		positionGroups[i] = group
	}

	d.pointQueue = make([]*data.Point, 0, d.queueSize) // 重新构建pointQueue，保持原来的分组结构但内部顺序已打乱
	maxLen := 0
	for _, group := range positionGroups {
		if len(group) > maxLen {
			maxLen = len(group)
		}
	}

	for groupIdx := 0; groupIdx < maxLen; groupIdx++ {
		for pointIdx := 0; pointIdx < d.Orderquantity; pointIdx++ {
			if groupIdx < len(positionGroups[pointIdx]) {
				d.pointQueue = append(d.pointQueue, positionGroups[pointIdx][groupIdx])
			}
		}
	}
}

// 处理乱序逻辑
func updateHostIndexAndTime(d *CPUOnlySimulator) {
	if d.hostIndex == uint64(d.Orderquantity*(d.number+1)) {
		begin := d.hostIndex - uint64(d.Orderquantity)
		end := d.hostIndex
		for i := begin; i < end; i++ {
			d.hosts[i].TickAll(d.interval)
		}
		d.currentTime = d.currentTime.Add(d.interval)
		if d.currentTime == d.timestampEnd {
			d.currentTime = d.timestampStart
			d.number++
			begin = end
			d.adjustNumHostsForEpoch()
		}
		d.hostIndex = begin
	}
	if d.hostIndex == uint64(len(d.hosts)) {
		begin := uint64(d.Orderquantity * (d.number))
		end := d.hostIndex
		for i := begin; i < end; i++ {
			d.hosts[i].TickAll(d.interval)
		}
		d.hostIndex = uint64(d.Orderquantity * (d.number))
	}
}

func copyPoint(p *data.Point) *data.Point {
	newP := data.NewPoint()
	newP.DeepCopy(p)
	return newP
}

// CPUOnlySimulatorConfig is used to create a CPUOnlySimulator.
type CPUOnlySimulatorConfig commonDevopsSimulatorConfig

// NewSimulator produces a Simulator that conforms to the given SimulatorConfig over the specified interval
func (c *CPUOnlySimulatorConfig) NewSimulator(interval time.Duration, limit uint64) common.Simulator {
	hostInfos := make([]Host, c.HostCount)
	for i := 0; i < len(hostInfos); {
		for j := i; j < c.Orderquantity+i && j < int(c.HostCount); j++ {
			hostInfos[j] = c.HostConstructor(NewHostCtx(j, c.Start))
		}
		i += c.Orderquantity
	}

	epochs := calculateEpochs(commonDevopsSimulatorConfig(*c), interval)
	maxPoints := epochs * c.HostCount
	if limit > 0 && limit < maxPoints {
		// Set specified points number limit
		maxPoints = limit
	}

	if int(c.End.Sub(c.Start)) < c.OutOfOrderWindow*int(interval) {
		panic(fmt.Sprintf("OutOfOrderWindow exceeds the time range"))
	}

	var qsize int
	if int(c.HostCount) <= c.Orderquantity {
		qsize = int(c.HostCount) * c.OutOfOrderWindow
	} else {
		qsize = c.Orderquantity * c.OutOfOrderWindow
	}

	sim := &CPUOnlySimulator{&commonDevopsSimulator{
		madePoints: 0,
		maxPoints:  maxPoints,

		hostIndex: 0,
		hosts:     hostInfos,

		epoch:          0,
		epochs:         epochs,
		epochHosts:     c.InitHostCount,
		initHosts:      c.InitHostCount,
		timestampStart: c.Start,
		timestampEnd:   c.End,
		interval:       interval,
		Orderquantity:  c.Orderquantity,
		OutOfOrder:     c.OutOfOrder,
		queueSize:      qsize,
	}, c.Start}

	return sim
}
