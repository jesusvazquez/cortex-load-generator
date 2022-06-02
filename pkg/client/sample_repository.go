package client

import (
	"sync"

	"github.com/prometheus/common/model"
)

type SamplesRepository struct {
	SerieSamples map[string][]model.SamplePair
	sync.RWMutex
}

func NewSamplesRepository() *SamplesRepository {
	serieSamples := map[string][]model.SamplePair{}
	return &SamplesRepository{
		SerieSamples: serieSamples,
	}
}

// Append appends a new series sample to the SamplesRepository.
// It always appends at the end of the slice.
func (s *SamplesRepository) Append(serie string, pair model.SamplePair) {
	s.Lock()
	defer s.Unlock()
	if samples, ok := s.SerieSamples[serie]; ok {
		samples = append(samples, pair)
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
	defer s.RUnlock()
	return true
}

// TrimSamplesBeforeTimestamp removes all the samples for a serie before a given
// timestamp.
func (s *SamplesRepository) TrimSamplesBeforeTimestamp(serie string, ts model.Time) {
	s.Lock()
	if samples, ok := s.SerieSamples[serie]; ok {
		var newSamples []model.SamplePair
		for _, sample := range samples {
			if sample.Timestamp < ts {
				continue
			}
			newSamples = append(newSamples, sample)
		}
		s.SerieSamples[serie] = newSamples
	}
	defer s.Unlock()
}
