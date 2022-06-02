package client

import (
	"sort"
	"sync"

	"github.com/prometheus/common/model"
)

type SamplesRepository struct {
	SerieSamples    map[string][]model.SamplePair
	KnownTimestamps map[model.Time]struct{}
	sync.RWMutex
}

func NewSamplesRepository() *SamplesRepository {
	serieSamples := make(map[string][]model.SamplePair)
	timestamps := make(map[model.Time]struct{})
	return &SamplesRepository{
		SerieSamples:    serieSamples,
		KnownTimestamps: timestamps,
	}
}

// Append appends a new series sample to the SamplesRepository.
// The samples will be kept in a sorted slice by min timestamp.
// If the repository already has a sample for a given timestamp it will not
// be appended.
func (s *SamplesRepository) Append(serie string, pair model.SamplePair) {
	s.Lock()
	defer s.Unlock()

	// If its a duplicate we dont append it
	if _, ok := s.KnownTimestamps[pair.Timestamp]; ok {
		return
	}
	s.KnownTimestamps[pair.Timestamp] = struct{}{}

	if samples, ok := s.SerieSamples[serie]; ok {
		samples = append(samples, pair)
		sort.Slice(samples, func(i, j int) bool {
			return samples[i].Timestamp < samples[j].Timestamp
		})
		s.SerieSamples[serie] = samples
	} else {
		s.SerieSamples[serie] = []model.SamplePair{pair}
	}
}

// MatchRepository checks if the given list of expectedSamples is a subset of the
// samples contained in the repository for a given serie.
// It is considered a matching subset if it matches all the samples contained
// in the repository in strict order.
// It will still match if the expectedSamples input misses up to the last two
// samples in the repository.
func (s *SamplesRepository) MatchRepository(serie string, expectedSamples []model.SamplePair) bool {
	s.RLock()
	defer s.RUnlock()
	if samples, ok := s.SerieSamples[serie]; ok {
		if len(expectedSamples) > len(samples) ||
			len(expectedSamples) < len(samples)-2 {
			return false
		}

		for i, sample := range expectedSamples {
			if samples[i].Value != sample.Value || samples[i].Timestamp != sample.Timestamp {
				return false
			}
		}
	} else {
		return len(expectedSamples) == 0
	}
	return true
}

// TrimSamplesBeforeTimestamp removes all the samples for a serie before a given
// timestamp.
func (s *SamplesRepository) TrimSamplesBeforeTimestamp(serie string, ts model.Time) {
	s.Lock()
	defer s.Unlock()
	if samples, ok := s.SerieSamples[serie]; ok {
		var newSamples []model.SamplePair
		for _, sample := range samples {
			if sample.Timestamp < ts {
				delete(s.KnownTimestamps, sample.Timestamp)
				continue
			}
			newSamples = append(newSamples, sample)
		}
		s.SerieSamples[serie] = newSamples
	}
}

func (s *SamplesRepository) Difference(serie string, inputSamples []model.SamplePair) []model.SamplePair {
	var diff []model.SamplePair
	tmp := make(map[model.Time]model.SamplePair)
	if samples, ok := s.SerieSamples[serie]; ok {
		for _, sample := range samples {
			tmp[sample.Timestamp] = sample
		}

		for _, sample := range inputSamples {
			if _, ok := tmp[sample.Timestamp]; !ok {
				diff = append(diff, sample)
			}
		}
	}
	return diff
}
