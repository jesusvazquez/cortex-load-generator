package client

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
)

const (
	maxComparisonDelta = 0.001

	comparisonSuccess    = "success"
	comparisonFailed     = "fail"
	oooComparisonSuccess = "ooo_success"
	oooComparisonFailed  = "ooo_fail"

	querySkipped    = "skipped"
	querySuccess    = "success"
	queryFailed     = "fail"
	oooQuerySkipped = "ooo_skipped"
	oooQuerySuccess = "ooo_success"
	oooQueryFailed  = "ooo_fail"
)

type QueryClientConfig struct {
	URL string

	// The tenant ID to use to push metrics to Cortex.
	UserID string

	QueryInterval time.Duration
	QueryTimeout  time.Duration
	QueryMaxAge   time.Duration

	ExpectedSeries        int
	ExpectedOOOSeries     int
	ExpectedWriteInterval time.Duration
}

type QueryClient struct {
	cfg       QueryClientConfig
	client    v1.API
	startTime time.Time
	logger    log.Logger

	// Metrics.
	queriesTotal         *prometheus.CounterVec
	resultsComparedTotal *prometheus.CounterVec
}

func NewQueryClient(cfg QueryClientConfig, logger log.Logger, reg prometheus.Registerer) *QueryClient {
	var rt http.RoundTripper = &http.Transport{}
	rt = &clientRoundTripper{userID: cfg.UserID, rt: rt}

	apiCfg := api.Config{
		Address:      cfg.URL,
		RoundTripper: rt,
	}

	client, err := api.NewClient(apiCfg)
	if err != nil {
		panic(err)
	}

	c := &QueryClient{
		cfg:       cfg,
		client:    v1.NewAPI(client),
		startTime: time.Now().UTC(),
		logger:    log.With(logger, "user", cfg.UserID),

		queriesTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "cortex_load_generator_queries_total",
			Help:        "Total number of attempted queries.",
			ConstLabels: map[string]string{"user": cfg.UserID},
		}, []string{"result"}),
		resultsComparedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "cortex_load_generator_query_results_compared_total",
			Help:        "Total number of query results compared.",
			ConstLabels: map[string]string{"user": cfg.UserID},
		}, []string{"result"}),
	}

	// Init metrics.
	for _, result := range []string{querySuccess, queryFailed, querySkipped, oooQuerySuccess, oooQueryFailed, oooQuerySkipped} {
		c.queriesTotal.WithLabelValues(result).Add(0)
	}
	for _, result := range []string{comparisonSuccess, comparisonFailed, oooComparisonSuccess, oooComparisonFailed} {
		c.resultsComparedTotal.WithLabelValues(result).Add(0)
	}

	return c
}

func (c *QueryClient) Start() {
	go c.run()
}

func (c *QueryClient) run() {
	c.runQueryAndVerifyResult()
	c.runOOOQueryAndVerifyResult()

	ticker := time.NewTicker(c.cfg.QueryInterval)

	for {
		select {
		case <-ticker.C:
			c.runQueryAndVerifyResult()
			c.runOOOQueryAndVerifyResult()
		}
	}
}

func (c *QueryClient) runQueryAndVerifyResult() {
	// Compute the query start/end time.
	start, end, ok := c.getQueryTimeRange(time.Now().UTC())
	if !ok {
		level.Debug(c.logger).Log("msg", "query skipped because of no eligible time range to query")
		c.queriesTotal.WithLabelValues(querySkipped).Inc()
		return
	}

	step := c.getQueryStep(start, end, c.cfg.ExpectedWriteInterval)

	samples, err := c.runQuery("sum(cortex_load_generator_sine_wave)", start, end, step)
	if err != nil {
		level.Error(c.logger).Log("msg", "failed to execute query", "err", err)
		c.queriesTotal.WithLabelValues(queryFailed).Inc()
		return
	}

	c.queriesTotal.WithLabelValues(querySuccess).Inc()

	err = verifySineWaveSamples(samples, c.cfg.ExpectedSeries, step)
	if err != nil {
		level.Warn(c.logger).Log("msg", "query result comparison failed", "err", err)
		c.resultsComparedTotal.WithLabelValues(comparisonFailed).Inc()
		return
	}

	c.resultsComparedTotal.WithLabelValues(comparisonSuccess).Inc()
}

func (c *QueryClient) runOOOQueryAndVerifyResult() {
	// Compute the query start/end time.
	start, end, ok := c.getQueryTimeRange(time.Now().UTC())
	if !ok {
		level.Debug(c.logger).Log("msg", "query skipped because of no eligible time range to query")
		c.queriesTotal.WithLabelValues(oooQuerySkipped).Inc()
		return
	}

	step := c.getQueryStep(start, end, c.cfg.ExpectedWriteInterval)

	samples, err := c.runQuery("sum(cortex_load_generator_out_of_order_sine_wave)", start, end, step)
	if err != nil {
		level.Error(c.logger).Log("msg", "failed to execute ooo query", "err", err)
		c.queriesTotal.WithLabelValues(oooQueryFailed).Inc()
		return
	}

	c.queriesTotal.WithLabelValues(oooQuerySuccess).Inc()

	err = verifySineWaveSampleValues(samples, c.cfg.ExpectedOOOSeries)
	if err != nil {
		level.Warn(c.logger).Log("msg", "ooo query result comparison failed", "err", err)
		c.resultsComparedTotal.WithLabelValues(oooComparisonFailed).Inc()
		return
	}

	c.resultsComparedTotal.WithLabelValues(oooComparisonSuccess).Inc()
}

func (c *QueryClient) runQuery(query string, start, end time.Time, step time.Duration) ([]model.SamplePair, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.QueryTimeout)
	defer cancel()

	value, _, err := c.client.QueryRange(ctx, "sum(cortex_load_generator_sine_wave)", v1.Range{
		Start: start,
		End:   end,
		Step:  step,
	})
	if err != nil {
		return nil, err
	}

	if value.Type() != model.ValMatrix {
		return nil, errors.New("was expecting to get a Matrix")
	}

	matrix, ok := value.(model.Matrix)
	if !ok {
		return nil, errors.New("failed to cast type to Matrix")
	}

	if len(matrix) != 1 {
		return nil, fmt.Errorf("expected 1 series in the result but got %d", len(matrix))
	}

	var result []model.SamplePair
	for _, stream := range matrix {
		result = append(result, stream.Values...)
	}

	return result, nil
}

func (c *QueryClient) getQueryTimeRange(now time.Time) (start, end time.Time, ok bool) {
	// Do not query the last 2 scape interval to give enough time to all write
	// requests to successfully complete.
	end = alignTimestampToInterval(now.Add(-2*c.cfg.ExpectedWriteInterval), c.cfg.ExpectedWriteInterval)

	// Do not query before the start time because the config may have been different (eg. number of series).
	// Also give a 2 write intervals grace period to let the initial writes to succeed and honor the configured max age.
	start = now.Add(-c.cfg.QueryMaxAge)
	if startTimeWithGrace := c.startTime.Add(2 * c.cfg.ExpectedWriteInterval); startTimeWithGrace.After(start) {
		start = startTimeWithGrace
	}
	start = alignTimestampToInterval(start, c.cfg.ExpectedWriteInterval)

	// The query should run only if we have a valid range to query.
	ok = end.After(start)

	return
}

func (c *QueryClient) getQueryStep(start, end time.Time, writeInterval time.Duration) time.Duration {
	const maxSamples = 1000

	// Compute the number of samples that we would have if we every single sample.
	actualSamples := end.Sub(start) / writeInterval
	if actualSamples <= maxSamples {
		return writeInterval
	}

	// Adjust the query step based on the max steps spread over the query time range,
	// rounding it to write interval.
	step := end.Sub(start) / time.Duration(maxSamples)
	step = ((step / writeInterval) + 1) * writeInterval

	return step
}

func verifySineWaveSamples(samples []model.SamplePair, expectedSeries int, expectedStep time.Duration) error {
	for idx, sample := range samples {
		ts := time.UnixMilli(int64(sample.Timestamp)).UTC()

		// Assert on value.
		expectedValue := generateSineWaveValue(ts)
		if !compareSampleValues(float64(sample.Value), expectedValue*float64(expectedSeries)) {
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

// verifySineWaveSampleValues checks only the correctness of values w.r.t. the timestamp.
func verifySineWaveSampleValues(samples []model.SamplePair, expectedSeries int) error {
	for _, sample := range samples {
		ts := time.UnixMilli(int64(sample.Timestamp)).UTC()

		// Assert on value.
		expectedValue := generateSineWaveValue(ts)
		if !compareSampleValues(float64(sample.Value), expectedValue*float64(expectedSeries)) {
			return fmt.Errorf("sample at timestamp %d (%s) has value %f while was expecting %f", sample.Timestamp, ts.String(), sample.Value, expectedValue)
		}
	}

	return nil
}

func compareSampleValues(actual, expected float64) bool {
	delta := math.Abs((actual - expected) / maxComparisonDelta)
	return delta < maxComparisonDelta
}
