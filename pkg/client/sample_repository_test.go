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
		expSamples   []model.SamplePair
	}{
		"append to an empty repository works": {
			repository:   NewSamplesRepository(),
			inputSerie:   "foo",
			inputSamples: testSamples(1),
			expSamples:   testSamples(1),
		},
		"append to a non empty repository works": {
			repository: testRepository("foo", testSamples(1)),
			inputSerie: "foo",
			inputSamples: []model.SamplePair{
				{
					Value:     1,
					Timestamp: 1,
				},
			},
			expSamples: append(testSamples(1), model.SamplePair{
				Value:     1,
				Timestamp: 1,
			}),
		},
		"repository appended samples are ordered": {
			repository: testRepository("foo", []model.SamplePair{{0, 0}}),
			inputSerie: "foo",
			inputSamples: []model.SamplePair{
				{
					Value:     2,
					Timestamp: 2,
				},
				{
					Value:     1,
					Timestamp: 1,
				},
			},
			expSamples: []model.SamplePair{
				{
					Value:     0,
					Timestamp: 0,
				},
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
		"duplicates are not appended": {
			repository: testRepository("foo", testSamples(1)),
			inputSerie: "foo",
			inputSamples: []model.SamplePair{
				{
					Value:     1,
					Timestamp: 1,
				},
				{
					Value:     1,
					Timestamp: 1,
				},
			},
			expSamples: testSamples(2),
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			for _, sample := range tc.inputSamples {
				tc.repository.Append(tc.inputSerie, sample)
			}
			assert.Equal(t, tc.expSamples, tc.repository.SerieSamples[tc.inputSerie])
		})
	}
}

func TestSamplesRepository_MatchRepository(t *testing.T) {
	tests := map[string]struct {
		test       string
		repository *SamplesRepository
		expSerie   string
		expSamples []model.SamplePair
		expMatch   bool
	}{
		"empty sample series match an empty repository": {
			repository: NewSamplesRepository(),
			expSerie:   "foo",
			expSamples: []model.SamplePair{},
			expMatch:   true,
		},
		"more input samples than repository samples does not match": {
			repository: NewSamplesRepository(),
			expSerie:   "foo",
			expSamples: []model.SamplePair{
				{
					Timestamp: 1,
					Value:     1,
				},
			},
			expMatch: false,
		},
		"same list of samples matches": {
			repository: testRepository("foo", testSamples(5)),
			expSerie:   "foo",
			expSamples: testSamples(5),
			expMatch:   true,
		},
		"same list of samples minus one is still contained": {
			repository: testRepository("foo", testSamples(5)),
			expSerie:   "foo",
			expSamples: testSamples(4),
			expMatch:   true,
		},
		"same list of samples minus two is still contained": {
			repository: testRepository("foo", testSamples(5)),
			expSerie:   "foo",
			expSamples: testSamples(3),
			expMatch:   true,
		},
		"same list of samples minus three or more is not contained": {
			repository: testRepository("foo", testSamples(5)),
			expSerie:   "foo",
			expSamples: testSamples(2),
			expMatch:   false,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, tc.expMatch, tc.repository.MatchRepository(tc.expSerie, tc.expSamples))
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

			// Make sure that the list of known timestamps is also trimmed
			for ts := range tc.repository.KnownTimestamps {
				assert.GreaterOrEqual(t, ts, tc.inputTimestamp, "known timestamp element timestamp should be greater or equal")
			}
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
