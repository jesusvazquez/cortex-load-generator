package expectation

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pracucci/cortex-load-generator/pkg/gen"
	"github.com/prometheus/common/model"
)

const maxComparisonDelta = 0.001

type Validator func([]model.SamplePair) error

// Expectation describes which responses are expected from the TSDB, and starting from when.
// The writer client can update the expectation and the reader can make assertions on it.
type Expectation struct {
	ValidFrom time.Time
	Data      map[string][]model.SamplePair // validations which specify, for a selector, all the specified points
	Funcs     map[string]Validator          // validations which specify, for a selector, a function to run on the response
	sync.Mutex
}

func NewExpectation() *Expectation {
	return &Expectation{
		Data:  make(map[string][]model.SamplePair),
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

func (e *Expectation) Validate(selector string, result []model.SamplePair) error {
	e.Lock()
	defer e.Unlock()
	now := time.Now()
	if e.ValidFrom.After(now) {
		time.Sleep(e.ValidFrom.Sub(now))
	}
	exp, ok := e.Data[selector]
	if ok {
		if diff := cmp.Diff(exp, result); diff != "" {
			return fmt.Errorf("expectation mismatch (-want +got):\n%s", diff)
		}
	}
	fn, ok := e.Funcs[selector]
	if ok {
		return fn(result)
	}
	return nil
}

// GetSineWaveSequenceValidator returns a validator which checks that the values are a contiguous sequence constituting a sine series multiplied by the given factor.
// TODO: validate start and end.
func GetSineWaveSequenceValidator(factor int, expectedStep time.Duration) Validator {
	return func(samples []model.SamplePair) error {
		for idx, sample := range samples {
			ts := time.UnixMilli(int64(sample.Timestamp)).UTC()

			// Assert on value.
			expectedValue := gen.Sine(ts)
			if !compareSampleValues(float64(sample.Value), expectedValue*float64(factor)) {
				return fmt.Errorf("sample at timestamp %d (%s) has value %f while was expecting %f", sample.Timestamp, ts.String(), sample.Value, expectedValue)
			}

			// Assert on sample timestamp. We expect no gaps.
			if idx > 0 {
				prevTs := time.UnixMilli(int64(samples[idx-1].Timestamp)).UTC()
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
