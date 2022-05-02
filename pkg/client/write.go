package client

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
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
	"github.com/prometheus/prometheus/pkg/gate"
	"github.com/prometheus/prometheus/prompb"
)

const (
	maxErrMsgLen = 256
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
	logger    log.Logger
}

func NewWriteClient(cfg WriteClientConfig, logger log.Logger) *WriteClient {
	var rt http.RoundTripper = &http.Transport{}
	rt = &clientRoundTripper{tenantID: cfg.TenantID, rt: rt}

	c := &WriteClient{
		client:    &http.Client{Transport: rt},
		cfg:       cfg,
		writeGate: gate.New(cfg.WriteConcurrency),
		logger:    logger,
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
	series2 := generateOOOSineWaveSeries(ts, c.cfg.OOOSeriesCount, c.cfg.MaxOOOTime, c.cfg.WriteInterval)
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
	value := generateSineWaveValue(t)

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

func generateOOOSineWaveSeries(t time.Time, oooSeriesCount, maxOOOMins int, interval time.Duration) []*prompb.TimeSeries {
	out := make([]*prompb.TimeSeries, 0, oooSeriesCount)
	for i := 1; i <= oooSeriesCount; i++ {
		diffMs := rand.Int63n(int64(maxOOOMins) * time.Minute.Milliseconds())
		ts := t.Add(-time.Duration(diffMs) * time.Millisecond)
		ts = alignTimestampToInterval(ts, interval)

		out = append(out, &prompb.TimeSeries{
			Labels: []*prompb.Label{{
				Name:  "__name__",
				Value: "cortex_load_generator_out_of_order_sine_wave",
			}, {
				Name:  "wave",
				Value: strconv.Itoa(i),
			}},
			Samples: []prompb.Sample{{
				Value:     generateSineWaveValue(ts),
				Timestamp: ts.UnixMilli(),
			}},
		})
	}

	return out
}

func generateSineWaveValue(t time.Time) float64 {
	period := float64(10 * time.Minute)
	radians := float64(t.UnixNano()) / period * 2 * math.Pi
	return math.Sin(radians)
}
