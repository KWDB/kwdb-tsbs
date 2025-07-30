package common

import (
	"github.com/timescale/tsbs/pkg/data"
	"reflect"
	"time"
)

// SimulatorConfig is an interface to create a Simulator from a time.Duration.
type SimulatorConfig interface {
	NewSimulator(time.Duration, uint64) Simulator
}

// BaseSimulatorConfig is used to create a BaseSimulator.
type BaseSimulatorConfig struct {
	// Start is the beginning time for the Simulator
	Start time.Time
	// End is the ending time for the Simulator
	End time.Time
	// InitGeneratorScale is the number of Generators to start with in the first reporting period
	InitGeneratorScale uint64
	// GeneratorScale is the total number of Generators to have in the last reporting period
	GeneratorScale uint64
	// GeneratorConstructor is the function used to create a new Generator given an id number and start time
	GeneratorConstructor func(i int, start time.Time) Generator

	Orderquantity int
}

func calculateEpochs(duration time.Duration, interval time.Duration) uint64 {
	return uint64(duration.Nanoseconds() / interval.Nanoseconds())
}

// NewSimulator produces a Simulator that conforms to the given config over the specified interval.
func (sc *BaseSimulatorConfig) NewSimulator(interval time.Duration, limit uint64) Simulator {
	generators := make([]Generator, sc.GeneratorScale)
	for i := 0; i < len(generators); i++ {
		generators[i] = sc.GeneratorConstructor(i, sc.Start)
	}

	epochs := calculateEpochs(sc.End.Sub(sc.Start), interval)
	maxPoints := epochs * sc.GeneratorScale * uint64(len(generators[0].Measurements()))
	if limit > 0 && limit < maxPoints {
		// Set specified points number limit
		maxPoints = limit
	}
	sim := &BaseSimulator{
		madePoints: 0,
		maxPoints:  maxPoints,

		generatorIndex: 0,
		generators:     generators,

		epoch:           0,
		epochs:          epochs,
		epochGenerators: sc.InitGeneratorScale,
		initGenerators:  sc.InitGeneratorScale,
		timestampStart:  sc.Start,
		timestampEnd:    sc.End,
		interval:        interval,

		simulatedMeasurementIndex: 0,

		Orderquantity: sc.Orderquantity,
	}

	return sim
}

type GeneratedDataHeaders struct {
	TagTypes  []string
	TagKeys   []string
	FieldKeys map[string][]string
}

// Simulator simulates a use case.
type Simulator interface {
	Finished() bool
	Next(*data.Point) bool
	Fields() map[string][]string
	TagKeys() []string
	TagTypes() []string
	Headers() *GeneratedDataHeaders
}

// BaseSimulator generates data similar to truck readings.
type BaseSimulator struct {
	madePoints uint64
	maxPoints  uint64

	generatorIndex uint64
	generators     []Generator

	epoch           uint64
	epochs          uint64
	epochGenerators uint64
	initGenerators  uint64

	timestampStart time.Time
	timestampEnd   time.Time
	interval       time.Duration

	simulatedMeasurementIndex int

	Orderquantity int
	orderindex    uint64
	lastindex     uint64
	timeindex     int
	deviceindex   int
	count         uint64
	shouldProcess bool
	cycleCheck    int
}

// Finished tells whether we have simulated all the necessary points.
func (s *BaseSimulator) Finished() bool {
	return s.madePoints >= s.maxPoints
}

// Next advances a Point to the next state in the generator.
func (s *BaseSimulator) Next(p *data.Point) bool {
	intervalCount := int(s.timestampEnd.Sub(s.timestampStart) / s.interval)

	if s.cycleCheck+1 == intervalCount*2 {
		s.orderindex = 0
	}

	if s.generatorIndex != 0 && s.generatorIndex%uint64(s.Orderquantity/2) == 0 ||
		s.generatorIndex == s.initGenerators {
		s.orderindex++
		if s.simulatedMeasurementIndex == 0 {
			s.generatorIndex = s.lastindex
			s.simulatedMeasurementIndex++
		} else {
			s.simulatedMeasurementIndex--
		}
		s.cycleCheck++
	}

	if s.generatorIndex%uint64(s.Orderquantity/2) != 0 && s.orderindex != 0 &&
		s.orderindex%2 == 0 && s.timeindex < intervalCount {
		s.simulatedMeasurementIndex, s.generatorIndex = 0, s.lastindex
		end := uint64(s.Orderquantity/2) + s.lastindex
		if end > s.initGenerators {
			end = s.initGenerators
		}
		for i := s.lastindex; i < end; i++ {
			s.generators[i].TickAll(s.interval)
		}
		s.timeindex++
		s.orderindex = 0
		s.shouldProcess = true
		s.adjustNumHostsForEpoch()
	}

	if int(s.generatorIndex) >= len(s.generators) {
		s.madePoints = s.maxPoints
		return false
	}
	generator := s.generators[s.generatorIndex]
	// Populate the Generator tags.
	for _, tag := range generator.Tags() {
		p.AppendTag(tag.Key, tag.Value)
	}
	// Populate measurement-specific tags and fields:
	generator.Measurements()[s.simulatedMeasurementIndex].ToPoint(p)
	if s.shouldProcess && s.timeindex != 0 && intervalCount >= s.timeindex {
		if s.timeindex == intervalCount {
			s.count++
		} else {
			s.generatorIndex = s.lastindex
		}
		s.lastindex = s.generatorIndex
		s.shouldProcess = false
	}

	if s.timeindex == intervalCount && s.simulatedMeasurementIndex == 1 &&
		int(s.generatorIndex+1)%(s.Orderquantity/2) == 0 {
		s.generatorIndex = uint64(s.Orderquantity/2)*s.count - 1
		s.lastindex = s.generatorIndex + 1
		s.timeindex, s.deviceindex, s.orderindex = 0, 0, 0
	}

	ret := s.generatorIndex < s.epochGenerators
	s.madePoints++
	s.generatorIndex++
	return ret
}

// Fields returns all the simulated measurements for the device.
func (s *BaseSimulator) Fields() map[string][]string {
	if len(s.generators) <= 0 {
		panic("cannot get fields because no Generators added")
	}

	toReturn := make(map[string][]string, len(s.generators))
	for _, sm := range s.generators[0].Measurements() {
		point := data.NewPoint()
		sm.ToPoint(point)
		fieldKeys := point.FieldKeys()
		fieldKeysAsStr := make([]string, len(fieldKeys))
		for i, k := range fieldKeys {
			fieldKeysAsStr[i] = string(k)
		}
		toReturn[string(point.MeasurementName())] = fieldKeysAsStr
	}

	return toReturn
}

// TagKeys returns all the tag keys for the device.
func (s *BaseSimulator) TagKeys() []string {
	if len(s.generators) <= 0 {
		panic("cannot get tag keys because no Generators added")
	}

	tags := s.generators[0].Tags()
	tagKeys := make([]string, len(tags))
	for i, tag := range tags {
		tagKeys[i] = string(tag.Key)
	}

	return tagKeys
}

// TagTypes returns the type for each tag, extracted from the generated values.
func (s *BaseSimulator) TagTypes() []string {
	if len(s.generators) <= 0 {
		panic("cannot get tag types because no Generators added")
	}

	tags := s.generators[0].Tags()
	types := make([]string, len(tags))
	for i, tag := range tags {
		types[i] = reflect.TypeOf(tag.Value).String()
	}

	return types
}

func (s *BaseSimulator) Headers() *GeneratedDataHeaders {
	return &GeneratedDataHeaders{
		TagTypes:  s.TagTypes(),
		TagKeys:   s.TagKeys(),
		FieldKeys: s.Fields(),
	}
}

// TODO(rrk) - Can probably turn this logic into a separate interface and implement other
// types of scale up, e.g., exponential
//
// To "scale up" the number of reporting items, we need to know when
// which epoch we are currently in. Once we know that, we can take the "missing"
// amount of scale -- i.e., the max amount of scale less the initial amount
// -- and add it in proportion to the percentage of epochs that have passed. This
// way we simulate all items at each epoch, but at the end of the function
// we check whether the point should be recorded by the calling process.
func (s *BaseSimulator) adjustNumHostsForEpoch() {
	s.epoch++
	missingScale := float64(uint64(len(s.generators)) - s.initGenerators)
	s.epochGenerators = s.initGenerators + uint64(missingScale*float64(s.epoch)/float64(s.epochs-1))
}

// SimulatedMeasurement simulates one measurement (e.g. Redis for DevOps).
type SimulatedMeasurement interface {
	Tick(time.Duration)
	ToPoint(*data.Point)
}
