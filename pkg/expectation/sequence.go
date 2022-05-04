package expectation

import (
	"sort"

	"github.com/prometheus/common/model"
)

// OOOChunk maintains samples in time-ascending order.
// Inserts for timestamps already seen, are dropped (until we need more advanced cases).
// Samples are stored uncompressed to allow easy sorting.
type Sequence struct {
	samples []model.SamplePair
}

func NewSequence() *Sequence {
	return &Sequence{}
}

// Insert inserts the sample such that order is maintained.
// Returns false if insert was not possible due to the same timestamp already existing.
func (o *Sequence) Insert(_t int64, _v float64) bool {
	t := model.Time(_t)
	v := model.SampleValue(_v)
	// find index of sample we should replace
	i := sort.Search(len(o.samples), func(i int) bool { return o.samples[i].Timestamp >= t })

	if i >= len(o.samples) {
		// none found. append it at the end
		o.samples = append(o.samples, model.SamplePair{t, v})
		return true
	}

	if o.samples[i].Timestamp == t {
		return false
	}

	// expand length by 1 to make room. use a zero sample, we will overwrite it anyway
	o.samples = append(o.samples, model.SamplePair{})
	copy(o.samples[i+1:], o.samples[i:])
	o.samples[i] = model.SamplePair{t, v}

	return true
}
