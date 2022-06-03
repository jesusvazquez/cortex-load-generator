package client

import (
	"sort"
	"sync"

	"github.com/prometheus/common/model"
)

type SamplesRepository struct {
	SerieSamples    map[string][]model.SamplePair
	KnownTimestamps map[string]map[model.Time]struct{}
	sync.RWMutex
}

func NewSamplesRepository() *SamplesRepository {
	serieSamples := make(map[string][]model.SamplePair)
	timestamps := make(map[string]map[model.Time]struct{})
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

	if kt, ok := s.KnownTimestamps[serie]; ok {
		if _, ok = kt[pair.Timestamp]; ok {
			return
		}
	} else {
		s.KnownTimestamps[serie] = make(map[model.Time]struct{})
	}
	s.KnownTimestamps[serie][pair.Timestamp] = struct{}{}

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
// in the repository.
// It will still match if the expectedSamples input misses up to the last two
// samples in the repository.
func (s *SamplesRepository) MatchRepository(serie string, expectedSamples []model.SamplePair) bool {
	s.RLock()
	defer s.RUnlock()
	if storedSamples, ok := s.SerieSamples[serie]; ok {
		if len(expectedSamples) > len(storedSamples) ||
			len(expectedSamples) < len(storedSamples)-2 {
			return false
		}

		match := 0
		for _, sample := range expectedSamples {
			for _, storedSample := range storedSamples {
				if storedSample.Value == sample.Value || storedSample.Timestamp == sample.Timestamp {
					match++
					break
				}
			}
		}
		return match >= len(expectedSamples)-2
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
		kts := s.KnownTimestamps[serie]
		for _, sample := range samples {
			if sample.Timestamp < ts {
				delete(kts, sample.Timestamp)
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
