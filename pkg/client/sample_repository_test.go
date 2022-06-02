package client

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
)

func TestSamplesRepository_Append(t *testing.T) {
	tests := map[string]struct {
		test         string
		repository   *SamplesRepository
		inputSerie   string
		inputSamples []model.SamplePair
	}{
		"append to an empty repository works": {
			repository: NewSamplesRepository(),
			inputSerie: "foo",
			inputSamples: []model.SamplePair{
				{
					Value:     1,
					Timestamp: 1,
				},
			},
		},
		"append to a non empty repository works": {
			repository: testRepository("foo", []model.SamplePair{{0, 0}}),
			inputSerie: "foo",
			inputSamples: []model.SamplePair{
				{
					Value:     1,
					Timestamp: 1,
				},
			},
		},
		"appends are ordered": {
			repository: testRepository("foo", []model.SamplePair{{0, 0}}),
			inputSerie: "foo",
			inputSamples: []model.SamplePair{
				{
					Value:     1,
					Timestamp: 1,
				},
				{
					Value:     2,
					Timestamp: 2,
				},
			},
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			initSamples := tc.repository.SerieSamples[tc.inputSerie]
			for _, sample := range tc.inputSamples {
				tc.repository.Append(tc.inputSerie, sample)
			}
			expSamples := append(initSamples, tc.inputSamples...)
			assert.Equal(t, tc.repository.SerieSamples[tc.inputSerie], expSamples)
		})
	}
}

func TestSamplesRepository_IsContained(t *testing.T) {
	tests := map[string]struct {
		test         string
		repository   *SamplesRepository
		expSerie     string
		expSamples   []model.SamplePair
		expContained bool
	}{
		"empty sample series is contained": {
			repository:   NewSamplesRepository(),
			expSerie:     "foo",
			expSamples:   []model.SamplePair{},
			expContained: true,
		},
		"more input samples than repository samples is not contained": {
			repository: NewSamplesRepository(),
			expSerie:   "foo",
			expSamples: []model.SamplePair{
				{
					Timestamp: 1,
					Value:     1,
				},
			},
			expContained: false,
		},
		"same list of samples is contained": {
			repository:   testRepository("foo", testSamples(5)),
			expSerie:     "foo",
			expSamples:   testSamples(5),
			expContained: true,
		},
		"same list of samples minus one is still contained": {
			repository:   testRepository("foo", testSamples(5)),
			expSerie:     "foo",
			expSamples:   testSamples(4),
			expContained: true,
		},
		"same list of samples minus two is still contained": {
			repository:   testRepository("foo", testSamples(5)),
			expSerie:     "foo",
			expSamples:   testSamples(3),
			expContained: true,
		},
		"same list of samples minus three or more is not contained": {
			repository:   testRepository("foo", testSamples(5)),
			expSerie:     "foo",
			expSamples:   testSamples(2),
			expContained: false,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, tc.expContained, tc.repository.IsContained(tc.expSerie, tc.expSamples))
		})
	}
}

func TestSamplesRepository_TrimSamplesBeforeTimestamp(t *testing.T) {
	tests := map[string]struct {
		test           string
		repository     *SamplesRepository
		inputSerie     string
		inputTimestamp model.Time
		expSamples     []model.SamplePair
	}{
		"trimming an empty repository results in an empty repository": {
			repository:     NewSamplesRepository(),
			inputSerie:     "foo",
			inputTimestamp: 0,
			expSamples:     []model.SamplePair(nil),
		},
		"trimming a non empty repository but no affected samples should leave the repository as it is": {
			repository:     testRepository("foo", testSamples(5)),
			inputSerie:     "foo",
			inputTimestamp: -1,
			expSamples:     testSamples(5),
		},
		"trimming a non empty repository removes all the affected samples": {
			repository:     testRepository("foo", testSamples(5)),
			inputSerie:     "foo",
			inputTimestamp: 3,
			expSamples: []model.SamplePair{
				{
					Timestamp: 3,
					Value:     3,
				},
				{
					Timestamp: 4,
					Value:     4,
				},
			},
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			tc.repository.TrimSamplesBeforeTimestamp(tc.inputSerie, tc.inputTimestamp)
			assert.Equal(t, tc.expSamples, tc.repository.SerieSamples[tc.inputSerie])

			// Trimming twice to make sure its consistent
			tc.repository.TrimSamplesBeforeTimestamp(tc.inputSerie, tc.inputTimestamp)
			assert.Equal(t, tc.expSamples, tc.repository.SerieSamples[tc.inputSerie])
		})
	}
}

func testRepository(serie string, initSamples []model.SamplePair) *SamplesRepository {
	repository := NewSamplesRepository()
	repository.SerieSamples[serie] = initSamples
	return repository
}

func testSamples(numSamples int) []model.SamplePair {
	var samples []model.SamplePair
	for i := 0; i < numSamples; i++ {
		samples = append(samples, model.SamplePair{Timestamp: model.Time(i), Value: model.SampleValue(i)})
	}
	return samples
}
