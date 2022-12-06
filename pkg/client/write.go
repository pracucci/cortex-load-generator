package client

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
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

	// The tenant ID to use to push metrics to Cortex.
	UserID string

	// Number of series to generate per write request.
	SeriesCount   int
	SawtoothCount int

	// Number of extra labels to generate per write request.
	ExtraLabels int

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
	rt = &clientRoundTripper{userID: cfg.UserID, rt: rt}

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
	ts := alignTimestampToInterval(time.Now(), c.cfg.WriteInterval)
	value := generateSineWaveValue(ts)
	series := generateSeries("cortex_load_generator_sine_wave", ts, value, c.cfg.SeriesCount, c.cfg.ExtraLabels)
	if c.cfg.SawtoothCount > 0 {
		value := generateSawtoothValue(ts)
		series = append(series, generateSeries("cortex_load_generator_sawtooth", ts, value, c.cfg.SawtoothCount, c.cfg.ExtraLabels)...)
	}

	// Honor the batch size.
	wg := sync.WaitGroup{}

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

func generateSeries(name string, t time.Time, value float64, seriesCount, extraLabels int) []*prompb.TimeSeries {
	out := make([]*prompb.TimeSeries, 0, seriesCount)

	for i := 1; i <= seriesCount; i++ {
		labels := []*prompb.Label{{
			Name:  "__name__",
			Value: name,
		}, {
			Name:  "wave",
			Value: strconv.Itoa(i),
		}}
		for j := 0; j < extraLabels; j++ {
			labels = append(labels, &prompb.Label{
				Name:  fmt.Sprintf("extraLabel%d", j),
				Value: "default",
			})
		}
		out = append(out, &prompb.TimeSeries{
			Labels: labels,
			Samples: []prompb.Sample{{
				Value:     value,
				Timestamp: t.UnixMilli(),
			}},
		})
	}

	return out
}

func generateSineWaveValue(t time.Time) float64 {
	// With a 15-second scrape interval this gives a ten-minute period
	period := float64(40 * (15 * time.Second))
	radians := float64(t.UnixNano()) / period * 2 * math.Pi
	return math.Sin(radians)
}

func generateSawtoothValue(t time.Time) float64 {
	// With a 15-second scrape interval this gives a ten-minute period
	period := float64(40 * (15 * time.Second))
	return math.Mod(float64(t.UnixNano()), period) / period
}
