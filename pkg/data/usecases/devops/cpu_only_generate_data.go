package devops

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
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
	// Switch to the next metric if needed
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
	return d.populatePoint(p, 0)
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
	}, c.Start}

	return sim
}
