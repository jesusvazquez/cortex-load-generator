package client

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/pkg/gate"
	"github.com/prometheus/prometheus/prompb"

	"github.com/pracucci/cortex-load-generator/pkg/expectation"
	"github.com/pracucci/cortex-load-generator/pkg/gen"
)

const (
	maxErrMsgLen = 256

	kindInOrder = "in-order"
	kindOOO     = "ooo"

	resOk   = "ok"
	resFail = "fail"
)

type WriteClientConfig struct {
	// Cortex URL.
	URL url.URL

	// The tenantID for pushing metrics.
	TenantID string

	// Number of series to generate per write request.
	SeriesCount    int
	OOOSeriesCount int

	MaxOOOTime int

	WriteInterval    time.Duration
	WriteTimeout     time.Duration
	WriteConcurrency int
	WriteBatchSize   int
}

type WriteClient struct {
	client    *http.Client
	cfg       WriteClientConfig
	writeGate *gate.Gate
	exp       *expectation.Expectation
	logger    log.Logger

	samplesTotal *prometheus.CounterVec
	reqTotal     *prometheus.CounterVec
}

func NewWriteClient(cfg WriteClientConfig, exp *expectation.Expectation, logger log.Logger, reg prometheus.Registerer) *WriteClient {
	var rt http.RoundTripper = &http.Transport{}
	rt = &clientRoundTripper{tenantID: cfg.TenantID, rt: rt}

	c := &WriteClient{
		client:    &http.Client{Transport: rt},
		cfg:       cfg,
		writeGate: gate.New(cfg.WriteConcurrency),
		exp:       exp,
		logger:    logger,

		samplesTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "cortex_load_generator_samples_written_total",
			Help:        "Total number of written samples.",
			ConstLabels: map[string]string{"tenant": cfg.TenantID},
		}, []string{"kind"}),

		reqTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "cortex_load_generator_write_req_total",
			Help:        "Total number of write requests.",
			ConstLabels: map[string]string{"tenant": cfg.TenantID},
		}, []string{"kind", "result"}),
	}

	// Init metrics.
	for _, kind := range []string{kindInOrder, kindOOO} {
		c.samplesTotal.WithLabelValues(kind).Add(0)
		for _, result := range []string{resOk, resFail} {
			c.reqTotal.WithLabelValues(kind, result).Add(0)
		}
	}

	go c.run()

	return c
}

func (c *WriteClient) run() {
	c.writeSeries()

	ticker := time.NewTicker(c.cfg.WriteInterval)

	for {
		select {
		case <-ticker.C:
			c.writeSeries()
		}
	}
}

func (c *WriteClient) writeSeries() {
	wg := &sync.WaitGroup{}
	writeSeries := func(series []*prompb.TimeSeries, kind string) {
		// Honor the batch size.
		for o := 0; o < len(series); o += c.cfg.WriteBatchSize {
			wg.Add(1)

			go func(o int) {
				defer wg.Done()

				// Honor the max concurrency
				ctx := context.Background()
				c.writeGate.Start(ctx)
				defer c.writeGate.Done()

				end := o + c.cfg.WriteBatchSize
				if end > len(series) {
					end = len(series)
				}

				req := &prompb.WriteRequest{
					Timeseries: series[o:end],
				}

				err := c.send(ctx, req, kind)
				if err != nil {
					level.Error(c.logger).Log("msg", "failed to write series", "kind", kind, "err", err)
					c.reqTotal.WithLabelValues(kind, resFail).Inc()
					return
				}
				c.reqTotal.WithLabelValues(kind, resOk).Inc()
				c.samplesTotal.WithLabelValues(kind).Add(float64(end - o))
			}(o)
		}
	}

	ts := alignTimestampToInterval(time.Now(), c.cfg.WriteInterval)
	series1, vals := generateSineWaveSeries(ts, c.cfg.SeriesCount, c.cfg.WriteInterval)
	series2, syn2 := generateOOOSineWaveSeries(ts, c.cfg.OOOSeriesCount, c.cfg.MaxOOOTime, c.cfg.WriteInterval)

	c.exp.Adjust(func(e *expectation.Expectation) {
		writeSeries(series1, kindInOrder)
		writeSeries(series2, kindOOO)

		e.Funcs = vals

		for selector, sample := range syn2 {
			_, ok := e.Data[selector]
			if !ok {
				e.Data[selector] = expectation.NewSequence()
			}
			e.Data[selector].Insert(sample.Timestamp, sample.Value)
		}

		wg.Wait()
		e.ValidFrom = time.Now().Add(2 * time.Second) // this should be enough for reads to include our writes.
	})
}

func (c *WriteClient) send(ctx context.Context, req *prompb.WriteRequest, kind string) error {
	for _, r := range req.Timeseries {
		for _, s := range r.Samples {
			level.Error(c.logger).Log("msg", "DIETER WRITE", "kind", kind, "name", r.Labels[0].Value, "ts", s.Timestamp, "v", s.Value)
		}
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	compressed := snappy.Encode(nil, data)
	httpReq, err := http.NewRequest("POST", c.cfg.URL.String(), bytes.NewReader(compressed))
	if err != nil {
		// Errors from NewRequest are from unparseable URLs, so are not
		// recoverable.
		return err
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", "cortex-load-generator")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	httpReq = httpReq.WithContext(ctx)

	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.WriteInterval)
	defer cancel()

	httpResp, err := c.client.Do(httpReq.WithContext(ctx))
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(httpResp.Body, maxErrMsgLen))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		err = fmt.Errorf("server returned HTTP status %s: %s", httpResp.Status, line)
	}
	if httpResp.StatusCode/100 == 5 {
		return err
	}
	return err
}

func alignTimestampToInterval(ts time.Time, interval time.Duration) time.Time {
	return time.Unix(0, (ts.UnixNano()/int64(interval))*int64(interval))
}

func generateSineWaveSeries(t time.Time, seriesCount int, step time.Duration) ([]*prompb.TimeSeries, map[string]expectation.Validator) {
	out := make([]*prompb.TimeSeries, 0, seriesCount)
	vals := make(map[string]expectation.Validator)

	for i := 1; i <= seriesCount; i++ {
		sample := prompb.Sample{
			Value:     gen.Sine(t) * float64(i),
			Timestamp: t.UnixMilli(),
		}
		out = append(out, &prompb.TimeSeries{
			Labels: []*prompb.Label{{
				Name:  "__name__",
				Value: "cortex_load_generator_sine_wave",
			}, {
				Name:  "wave",
				Value: strconv.Itoa(i),
			}},
			Samples: []prompb.Sample{sample},
		})
		vals[fmt.Sprintf("cortex_load_generator_sine_wave{wave=\"%d\"}", i)] = expectation.GetSineWaveSequenceValidator(i, step)
	}

	return out, vals
}

func generateOOOSineWaveSeries(t time.Time, oooSeriesCount, maxOOOMins int, interval time.Duration) ([]*prompb.TimeSeries, map[string]prompb.Sample) {
	out := make([]*prompb.TimeSeries, 0, oooSeriesCount)
	synopsis := make(map[string]prompb.Sample)
	for i := 1; i <= oooSeriesCount; i++ {
		diffMs := rand.Int63n(int64(maxOOOMins) * time.Minute.Milliseconds())
		ts := t.Add(-time.Duration(diffMs) * time.Millisecond)
		ts = alignTimestampToInterval(ts, interval)

		sample := prompb.Sample{
			Value:     gen.Sine(ts) * float64(i),
			Timestamp: ts.UnixMilli(),
		}

		out = append(out, &prompb.TimeSeries{
			Labels: []*prompb.Label{
				{
					Name:  "__name__",
					Value: "cortex_load_generator_out_of_order_sine_wave",
				},
				{
					Name:  "wave",
					Value: strconv.Itoa(i),
				},
			},
			Samples: []prompb.Sample{sample},
		})
		synopsis[fmt.Sprintf("cortex_load_generator_out_of_order_sine_wave{wave=\"%d\"}", i)] = sample
	}

	return out, synopsis
}
