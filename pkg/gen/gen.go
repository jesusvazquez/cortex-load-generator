package gen

import (
	"math"
	"time"
)

// Sine returns the sine for the given t with period of 10 minutes
func Sine(t time.Time) float64 {
	period := float64(10 * time.Minute)
	radians := float64(t.UnixNano()) / period * 2 * math.Pi
	return math.Sin(radians)
}
