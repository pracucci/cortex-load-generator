package client

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

const (
	maxErrMsgLen = 256
)

type ClientConfig struct {
	// Cortex URL.
	URL url.URL

	// The tenant ID to use to push metrics to Cortex.
	UserID string

	// Number of series to generate per write request.
	SeriesCount int

	WriteInterval time.Duration
	WriteTimeout  time.Duration
}

type Client struct {
	client *http.Client
	cfg    ClientConfig
}

func NewClient(cfg ClientConfig) *Client {
	var rt http.RoundTripper = &http.Transport{}
	rt = &clientRoundTripper{userID: cfg.UserID, rt: rt}

	c := &Client{
		client: &http.Client{Transport: rt},
		cfg:    cfg,
	}

	go c.run()

	return c
}

func (c *Client) run() {
	c.writeSeries()

	ticker := time.NewTicker(c.cfg.WriteInterval)

	for {
		select {
		case <-ticker.C:
			c.writeSeries()
		}
	}
}

func (c *Client) writeSeries() {
	series := generateSineWaveSeries(time.Now(), c.cfg.SeriesCount)

	req := &prompb.WriteRequest{
		Timeseries: series,
	}

	err := c.send(context.Background(), req)
	if err != nil {
		log.Println("Error while writing series: " + err.Error())
	}
}

func (c *Client) send(ctx context.Context, req *prompb.WriteRequest) error {
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
				Timestamp: int64(t.UnixNano() / int64(time.Millisecond)),
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
