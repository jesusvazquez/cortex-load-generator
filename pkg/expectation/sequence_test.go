package expectation

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/common/model"
)

func TestSequenceTrimLeft(t *testing.T) {
	for i, c := range []struct {
		in  []model.SamplePair
		t   int64
		out []model.SamplePair
	}{
		{
			in:  nil,
			t:   5,
			out: nil,
		},
		{
			in:  []model.SamplePair{{10, 0}},
			t:   9,
			out: []model.SamplePair{{10, 0}},
		},
		{
			in:  []model.SamplePair{{10, 0}},
			t:   10,
			out: []model.SamplePair{{10, 0}},
		},
		{
			in:  []model.SamplePair{{10, 0}},
			t:   11,
			out: []model.SamplePair{},
		},
		{
			in:  []model.SamplePair{{10, 0}, {15, 1}},
			t:   11,
			out: []model.SamplePair{{15, 1}},
		},
	} {
		seq := &Sequence{
			samples: c.in,
		}
		seq.TrimLeft(c.t)
		if diff := cmp.Diff(c.out, seq.samples); diff != "" {
			t.Errorf("case %d: Sequence.Trim() mismatch (-want +got):\n%s", i, diff)
		}
	}
}

func TestSequenceTrimRight(t *testing.T) {
	for i, c := range []struct {
		in  []model.SamplePair
		t   int64
		out []model.SamplePair
	}{
		{
			in:  nil,
			t:   5,
			out: nil,
		},
		{
			in:  []model.SamplePair{{10, 0}},
			t:   11,
			out: []model.SamplePair{{10, 0}},
		},
		{
			in:  []model.SamplePair{{10, 0}},
			t:   10,
			out: []model.SamplePair{{10, 0}},
		},
		{
			in:  []model.SamplePair{{10, 0}},
			t:   9,
			out: []model.SamplePair{},
		},
		{
			in:  []model.SamplePair{{10, 0}, {15, 1}},
			t:   11,
			out: []model.SamplePair{{10, 0}},
		},
	} {
		seq := &Sequence{
			samples: c.in,
		}
		seq.TrimRight(c.t)
		if diff := cmp.Diff(c.out, seq.samples); diff != "" {
			t.Errorf("case %d: Sequence.Trim() mismatch (-want +got):\n%s", i, diff)
		}
	}
}
