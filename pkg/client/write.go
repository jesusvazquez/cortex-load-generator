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
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/gate"
	"github.com/prometheus/prometheus/prompb"

	"github.com/pracucci/cortex-load-generator/pkg/gen"
)

const (
	maxErrMsgLen = 256

	kindNormal = "normal"
	kindOOO    = "ooo"

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
	client            *http.Client
	cfg               WriteClientConfig
	writeGate         *gate.Gate
	samplesRepository *SamplesRepository
	logger            log.Logger

	samplesTotal *prometheus.CounterVec
	reqTotal     *prometheus.CounterVec
}

func NewWriteClient(cfg WriteClientConfig, samplesRepository *SamplesRepository, logger log.Logger, reg prometheus.Registerer) *WriteClient {
	var rt http.RoundTripper = &http.Transport{}
	rt = &clientRoundTripper{tenantID: cfg.TenantID, rt: rt}

	c := &WriteClient{
		client:            &http.Client{Transport: rt},
		cfg:               cfg,
		writeGate:         gate.New(cfg.WriteConcurrency),
		samplesRepository: samplesRepository,
		logger:            logger,

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
	for _, kind := range []string{kindNormal, kindOOO} {
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
	wg := sync.WaitGroup{}
	writeSeries := func(series []*prompb.TimeSeries) {
		// Honor the batch size.
		for o := 0; o < len(series); o += c.cfg.WriteBatchSize {
			wg.Add(1)

			go func(o int) {
				defer wg.Done()

				// Honow the max concurrency
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

				err := c.send(ctx, req)
				if err != nil {
					level.Error(c.logger).Log("msg", "failed to write series", "err", err)
				}
			}(o)
		}
	}

	ts := alignTimestampToInterval(time.Now(), c.cfg.WriteInterval)
	series1 := generateSineWaveSeries(ts, c.cfg.SeriesCount)
	series2 := generateOOOSineWaveSeries(ts, c.cfg.OOOSeriesCount, c.cfg.MaxOOOTime, c.cfg.WriteInterval, c.samplesRepository)
	writeSeries(series1)
	writeSeries(series2)

	wg.Wait()
}

func (c *WriteClient) send(ctx context.Context, req *prompb.WriteRequest) error {
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

func generateSineWaveSeries(t time.Time, seriesCount int) []*prompb.TimeSeries {
	out := make([]*prompb.TimeSeries, 0, seriesCount)
	value := gen.Sine(t)

	for i := 1; i <= seriesCount; i++ {
		out = append(out, &prompb.TimeSeries{
			Labels: []*prompb.Label{{
				Name:  "__name__",
				Value: "cortex_load_generator_sine_wave",
			}, {
				Name:  "wave",
				Value: strconv.Itoa(i),
			}},
			Samples: []prompb.Sample{{
				Value:     value,
				Timestamp: t.UnixMilli(),
			}},
		})
	}
	return out
}

func generateOOOSineWaveSeries(t time.Time, oooSeriesCount, maxOOOMins int, interval time.Duration, samplesRepository *SamplesRepository) []*prompb.TimeSeries {
	out := make([]*prompb.TimeSeries, 0, oooSeriesCount)
	for i := 1; i <= oooSeriesCount; i++ {
		diffMs := rand.Int63n(int64(maxOOOMins) * time.Minute.Milliseconds())
		ts := t.Add(-time.Duration(diffMs) * time.Millisecond)
		ts = alignTimestampToInterval(ts, interval)

		sTimestamp := ts.UnixMilli()
		sValue := gen.Sine(ts)

		out = append(out, &prompb.TimeSeries{
			Labels: []*prompb.Label{{
				Name:  "__name__",
				Value: "cortex_load_generator_out_of_order_sine_wave",
			}, {
				Name:  "wave",
				Value: strconv.Itoa(i),
			}},
			Samples: []prompb.Sample{{
				Value:     sValue,
				Timestamp: sTimestamp,
			}},
		})

		// Keep track of OOO samples in the repository
		samplesRepository.Append(
			fmt.Sprintf("cortex_load_generator_out_of_order_sine_wave{wave=\"%d\"}", i),
			model.SamplePair{
				Timestamp: model.Time(sTimestamp),
				Value:     model.SampleValue(sValue),
			},
		)
	}

	return out
}
