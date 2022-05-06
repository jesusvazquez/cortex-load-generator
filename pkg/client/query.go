package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pracucci/cortex-load-generator/pkg/expectation"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
)

const (
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

	// The tenantID for querying metrics.
	TenantID string

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
	exp       *expectation.Expectation
	logger    log.Logger

	// Metrics.
	queriesTotal         *prometheus.CounterVec
	resultsComparedTotal *prometheus.CounterVec
}

func NewQueryClient(cfg QueryClientConfig, exp *expectation.Expectation, logger log.Logger, reg prometheus.Registerer) *QueryClient {
	var rt http.RoundTripper = &http.Transport{}
	rt = &clientRoundTripper{tenantID: cfg.TenantID, rt: rt}

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
		exp:       exp,
		logger:    log.With(logger, "tenant", cfg.TenantID),

		queriesTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "cortex_load_generator_queries_total",
			Help:        "Total number of attempted queries.",
			ConstLabels: map[string]string{"tenant": cfg.TenantID},
		}, []string{"result"}),
		resultsComparedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "cortex_load_generator_query_results_compared_total",
			Help:        "Total number of query results compared.",
			ConstLabels: map[string]string{"tenant": cfg.TenantID},
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
	go func() {
		c.run()
		ticker := time.NewTicker(c.cfg.QueryInterval)
		for range ticker.C {
			c.run()
		}
	}()

}

func (c *QueryClient) run() {
	// Compute the query start/end time.
	_, end, ok := c.getQueryTimeRange(time.Now().UTC())
	if !ok {
		level.Debug(c.logger).Log("msg", "queries skipped because of no eligible time range to query")
		c.queriesTotal.WithLabelValues(querySkipped).Add(float64(c.cfg.ExpectedSeries))
		c.queriesTotal.WithLabelValues(oooQuerySkipped).Add(float64(c.cfg.ExpectedOOOSeries))
		return
	}

	// TODO: something more clever than running all these queries at once. e.g. smoothing over time and/or only querying a subset
	for i := 1; i <= c.cfg.ExpectedSeries; i++ {
		q := fmt.Sprintf("cortex_load_generator_sine_wave{wave=\"%d\"}", i)
		c.runQueryAndVerifyResult(q, end, queryFailed, querySuccess, comparisonSuccess, comparisonFailed)
	}
	for i := 1; i <= c.cfg.ExpectedOOOSeries; i++ {
		qOOO := fmt.Sprintf("cortex_load_generator_out_of_order_sine_wave{wave=\"%d\"}", i)
		c.runQueryAndVerifyResult(qOOO, end, oooQueryFailed, oooQuerySuccess, oooComparisonSuccess, oooComparisonFailed)
	}
}

func (c *QueryClient) runQueryAndVerifyResult(query string, end time.Time, lblFail, lblSuccess, lblMatch, lblNomatch string) {

	// TODO make this configurable and make it always match the [60s] below
	start := end.Add(-time.Minute)

	samples, err := c.runQuery(query+"[60s]", end)
	if err != nil {
		level.Error(c.logger).Log("msg", "failed to execute query", "err", err)
		c.queriesTotal.WithLabelValues(lblFail).Inc()
		return
	}

	c.queriesTotal.WithLabelValues(lblSuccess).Inc()

	err = c.exp.Validate(query, start, end, samples)
	if err != nil {
		level.Warn(c.logger).Log("msg", "query result comparison failed", "start", start.UnixMilli(), "end", end.UnixMilli(), "err", err)
		c.resultsComparedTotal.WithLabelValues(lblNomatch).Inc()
		return
	}

	c.resultsComparedTotal.WithLabelValues(lblMatch).Inc()
}

func (c *QueryClient) runQuery(query string, ts time.Time) ([]model.SamplePair, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.QueryTimeout)
	defer cancel()

	value, _, err := c.client.Query(ctx, query, ts)
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

		return nil, fmt.Errorf("query %q with ts %v (now is %v) -> expected 1 series in the result but got %d", query, ts, time.Now(), len(matrix))
	}

	return matrix[0].Values, nil
}

func (c *QueryClient) getQueryTimeRange(now time.Time) (start, end time.Time, ok bool) {
	// Do not query the last 2 scape interval to give enough time to all write
	// requests to successfully complete.

	// set end between now-2*writeInterval and now-3writeInterval
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
