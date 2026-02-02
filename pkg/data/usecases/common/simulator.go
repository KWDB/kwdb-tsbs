package common

import (
	"reflect"
	"time"

	"github.com/timescale/tsbs/pkg/data"
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
	// Orderquantity is the batch size for generating data points
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

	measurementCount := len(generators[0].Measurements())
	epochs := calculateEpochs(sc.End.Sub(sc.Start), interval)
	maxPoints := epochs * sc.GeneratorScale * uint64(measurementCount)
	if limit > 0 && limit < maxPoints {
		// Set specified points number limit
		maxPoints = limit
	}

	orderquantity := sc.Orderquantity
	if orderquantity <= 0 {
		orderquantity = int(sc.InitGeneratorScale) // default to all generators
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
		measurementCount:          measurementCount,
		orderquantity:             orderquantity,
		batchStart:                0,
		batchIndex:                0,
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
// Data generation order (Scenario B): each batch completes all time points before moving to next batch.
// Example with Orderquantity=12, 24 devices, 3 time points:
//
//	t0 readings[0..11] -> t0 diagnostics[0..11] -> t1 readings[0..11] -> t1 diagnostics[0..11] -> ... -> t2 diagnostics[0..11]
//	t0 readings[12..23] -> t0 diagnostics[12..23] -> t1 readings[12..23] -> ... -> t2 diagnostics[12..23]
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
	measurementCount          int
	timeindex                 int

	orderquantity int    // batch size for generating data points
	batchStart    uint64 // start index of current batch
	batchIndex    int    // current index within the batch
}

// Finished tells whether we have simulated all the necessary points.
func (s *BaseSimulator) Finished() bool {
	return s.madePoints >= s.maxPoints
}

// Next advances a Point to the next state in the generator.
// Generation order (Scenario B): batch -> time -> measurement -> generator within batch
// Each batch of Orderquantity generators completes all time points before moving to next batch.
func (s *BaseSimulator) Next(p *data.Point) bool {
	intervalCount := int(s.timestampEnd.Sub(s.timestampStart) / s.interval)

	// Calculate current generator index
	s.generatorIndex = s.batchStart + uint64(s.batchIndex)

	// Calculate effective batch size (may be smaller for last batch)
	batchEnd := s.batchStart + uint64(s.orderquantity)
	if batchEnd > s.epochGenerators {
		batchEnd = s.epochGenerators
	}
	effectiveBatchSize := int(batchEnd - s.batchStart)

	// Check if current batch within current measurement is complete
	if s.batchIndex >= effectiveBatchSize {
		s.batchIndex = 0
		s.simulatedMeasurementIndex++

		// Check if all measurements for current time point are complete
		if s.simulatedMeasurementIndex >= s.measurementCount {
			s.simulatedMeasurementIndex = 0

			// Advance time for current batch of generators only
			for i := s.batchStart; i < batchEnd; i++ {
				s.generators[i].TickAll(s.interval)
			}
			s.timeindex++

			// Check if all time points for current batch are complete
			if s.timeindex >= intervalCount {
				s.timeindex = 0
				s.batchStart += uint64(s.orderquantity)
			}
		}
		// Recalculate generator index after state changes
		s.generatorIndex = s.batchStart + uint64(s.batchIndex)
	}

	// Check if simulation is complete (all batches processed)
	if s.batchStart >= s.epochGenerators {
		s.madePoints = s.maxPoints
		return false
	}

	// Generate current data point
	generator := s.generators[s.generatorIndex]
	// Populate the Generator tags
	for _, tag := range generator.Tags() {
		p.AppendTag(tag.Key, tag.Value)
	}
	// Populate measurement-specific tags and fields
	generator.Measurements()[s.simulatedMeasurementIndex].ToPoint(p)

	s.madePoints++
	s.batchIndex++
	return true
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
