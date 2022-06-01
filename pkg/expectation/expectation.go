package expectation

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/common/model"

	"github.com/pracucci/cortex-load-generator/pkg/gen"
)

const maxComparisonDelta = 0.001

type Validator func(start, end time.Time, result []model.SamplePair) error

// Expectation describes which responses are expected from the TSDB, and starting from when.
// The writer client can update the expectation and the reader can make assertions on it.
type Expectation struct {
	ValidFrom time.Time            // wallclock time at which queries should see the data that was written when this validFrom was set.
	Data      map[string]*Sequence // validations which specify, for a selector, all the specified samples
	Funcs     map[string]Validator // validations which specify, for a selector, a function to run on the response
	sync.Mutex
}

func NewExpectation() *Expectation {
	return &Expectation{
		Data:  make(map[string]*Sequence),
		Funcs: make(map[string]Validator),
	}
}

// Adjustment lets a write client adjust the expectation
type Adjustment func(e *Expectation)

func (e *Expectation) Adjust(fn Adjustment) {
	e.Lock()
	fn(e)
	e.Unlock()
}

func (e *Expectation) Validate(selector string, start, end time.Time, result []model.SamplePair) error {
	e.Lock()
	defer e.Unlock()
	now := time.Now()
	if e.ValidFrom.After(now) {
		time.Sleep(e.ValidFrom.Sub(now))
	}
	exp, ok := e.Data[selector]
	if ok {
		exp.TrimLeft(start.UnixMilli())
		exp.TrimRight(end.UnixMilli())

		sort.Slice(exp.samples, func(i, j int) bool {
			return exp.samples[i].Timestamp < exp.samples[j].Timestamp
		})
		sort.Slice(result, func(i, j int) bool {
			return result[i].Timestamp < result[j].Timestamp
		})

		// Let's consider two cases here.
		// If we got all the expeted samples in the result thats almost what we want so lets differentiate
		diff := cmp.Diff(exp.samples, result)
		if len(exp.samples) > len(result) {
			return fmt.Errorf("expectation mismatch 1 - len(EXP) > len(GOT) (%d>%d) and GOT should be at least same size  \nEXP%s\nGOT%s\n-want +got):\n%s", len(exp.samples), len(result), exp.samples, result, diff)
		}

		for i := 0; i < len(exp.samples); i++ {
			found := false
			for j := 0; j < len(result); j++ {
				if exp.samples[i].Value == result[j].Value && exp.samples[i].Timestamp == result[j].Timestamp {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("expectation mismatch 2 - at least one expected sample was not found \nEXP%s\nGOT%s\n-want +got):\n%s", exp.samples, result, diff)
			}
		}

		if diff != "" {
			return fmt.Errorf("expectation mismatch 3 - results are different, dont know why yet\nEXP%s\nGOT%s\n-want +got):\n%s", exp.samples, result, diff)
		}
	}
	fn, ok := e.Funcs[selector]
	if ok {
		return fn(start, end, result)
	}
	return nil
}

// GetSineWaveSequenceValidator returns a validator which checks that the values are a contiguous sequence constituting a sine series multiplied by the given factor.
// TODO: validate start and end.
func GetSineWaveSequenceValidator(factor int, expectedStep time.Duration) Validator {
	return func(start, end time.Time, result []model.SamplePair) error {
		for idx, sample := range result {
			ts := time.UnixMilli(int64(sample.Timestamp)).UTC()

			// Assert on value.
			expectedValue := gen.Sine(ts)
			if !compareSampleValues(float64(sample.Value), expectedValue*float64(factor)) {
				return fmt.Errorf("sample at timestamp %d (%s) has value %f while was expecting %f", sample.Timestamp, ts.String(), sample.Value, expectedValue)
			}

			// Assert on sample timestamp. We expect no gaps.
			if idx > 0 {
				prevTs := time.UnixMilli(int64(result[idx-1].Timestamp)).UTC()
				expectedTs := prevTs.Add(expectedStep)

				if ts.UnixMilli() != expectedTs.UnixMilli() {
					return fmt.Errorf("sample at timestamp %d (%s) was expected to have timestamp %d (%s) because previous sample had timestamp %d (%s)",
						sample.Timestamp, ts.String(), expectedTs.UnixMilli(), expectedTs.String(), prevTs.UnixMilli(), prevTs.String())
				}
			}
		}

		return nil
	}
}

func compareSampleValues(actual, expected float64) bool {
	delta := math.Abs((actual - expected) / maxComparisonDelta)
	return delta < maxComparisonDelta
}
