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
	"sort"
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
	SeriesCount int

	// SeriesChurnPeriod is the time period during which all series gradually churn.
	// 0 to disable churning.
	SeriesChurnPeriod time.Duration

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

	return c
}

func (c *WriteClient) Start() {
	go c.run()
}

func (c *WriteClient) run() {
	c.writeSeries()

	ticker := time.NewTicker(c.cfg.WriteInterval)

	for range ticker.C {
		c.writeSeries()
	}
}

func (c *WriteClient) writeSeries() {
	ts := alignTimestampToInterval(time.Now(), c.cfg.WriteInterval)
	series := generateSineWaveSeries(ts, c.cfg.SeriesCount, c.cfg.ExtraLabels, c.cfg.SeriesChurnPeriod)

	// Honor the batch size.
	wg := sync.WaitGroup{}

	for o := 0; o < len(series); o += c.cfg.WriteBatchSize {
		wg.Add(1)

		go func(o int) {
			defer wg.Done()

			// Honor the max concurrency
			ctx := context.Background()
			_ = c.writeGate.Start(ctx)
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

func generateSineWaveSeries(t time.Time, seriesCount, extraLabelsCount int, churnPeriod time.Duration) []*prompb.TimeSeries {
	out := make([]*prompb.TimeSeries, 0, seriesCount)
	value := generateSineWaveValue(t)

	// Generate the extra labels.
	extraLabels := make([]*prompb.Label, 0, extraLabelsCount)
	for j := 0; j < extraLabelsCount; j++ {
		extraLabels = append(extraLabels, &prompb.Label{
			Name:  fmt.Sprintf("extraLabel%d", j),
			Value: "default",
		})
	}

	for seriesID := 1; seriesID <= seriesCount; seriesID++ {
		labels := make([]*prompb.Label, 0, 3+extraLabelsCount)
		labels = append(labels, &prompb.Label{
			Name:  "__name__",
			Value: "cortex_load_generator_sine_wave",
		}, &prompb.Label{
			Name:  "wave",
			Value: strconv.Itoa(seriesID),
		})

		// Add extra labels.
		labels = append(labels, extraLabels...)

		// Add a label to simulate churning series.
		if churnPeriod > 0 {
			// Spread churning series over the "churn period" we compute the churn ID
			// starting from the current time, shifted by the series ID. Then the value
			// is rounded so that it changes every "churn period".
			churnID := t.Add((churnPeriod/time.Duration(seriesCount))*time.Duration(seriesID)).Unix() / int64(churnPeriod.Seconds())

			labels = append(labels, &prompb.Label{
				Name:  "churn",
				Value: fmt.Sprintf("%d", churnID),
			})
		}

		// Ensure labels are sorted.
		sort.Slice(labels, func(i, j int) bool {
			if labels[i].Name != labels[j].Name {
				return labels[i].Name < labels[j].Name
			}
			return labels[i].Value < labels[j].Value
		})

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
