package client

import (
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
)

func TestQueryClient_GetQueryTimeRange(t *testing.T) {
	now := time.Now()

	tests := map[string]struct {
		cfg           QueryClientConfig
		now           time.Time
		startTime     time.Time
		expectedOK    bool
		expectedStart time.Time
		expectedEnd   time.Time
	}{
		"should not run a query if client has just started": {
			cfg:        QueryClientConfig{ExpectedWriteInterval: 10 * time.Second},
			now:        now,
			startTime:  now,
			expectedOK: false,
		},
		"should add a grace period to start and not query the last 2 write intervals": {
			cfg:           QueryClientConfig{ExpectedWriteInterval: 10 * time.Second, QueryMaxAge: 2 * time.Hour},
			now:           now,
			startTime:     now.Add(-1 * time.Hour),
			expectedOK:    true,
			expectedStart: alignTimestampToInterval(now.Add(-1*time.Hour).Add(2*10*time.Second), 10*time.Second),
			expectedEnd:   alignTimestampToInterval(now.Add(-2*10*time.Second), 10*time.Second),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			client := NewQueryClient(testData.cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry())
			client.startTime = testData.startTime

			actualStart, actualEnd, actualOK := client.getQueryTimeRange(testData.now)
			assert.Equal(t, testData.expectedOK, actualOK)

			if testData.expectedOK {
				assert.Equal(t, testData.expectedStart, actualStart)
				assert.Equal(t, testData.expectedEnd, actualEnd)
			}
		})
	}
}

func TestVerifySineWaveSamples(t *testing.T) {
	// Round to millis since that's the precision of Prometheus timestamps.
	now := time.UnixMilli(time.Now().UnixMilli()).UTC()

	tests := map[string]struct {
		samples               []model.SamplePair
		expectedSeries        int
		expectedWriteInterval time.Duration
		expectedErr           string
	}{
		"should return no error if all samples value and timestamp match the expected one (1 series)": {
			samples: []model.SamplePair{
				newSamplePair(now.Add(10*time.Second), generateSineWaveValue(now.Add(10*time.Second))),
				newSamplePair(now.Add(20*time.Second), generateSineWaveValue(now.Add(20*time.Second))),
				newSamplePair(now.Add(30*time.Second), generateSineWaveValue(now.Add(30*time.Second))),
			},
			expectedSeries:        1,
			expectedWriteInterval: 10 * time.Second,
			expectedErr:           "",
		},
		"should return no error if all samples value and timestamp match the expected one (multiple series)": {
			samples: []model.SamplePair{
				newSamplePair(now.Add(10*time.Second), 5*generateSineWaveValue(now.Add(10*time.Second))),
				newSamplePair(now.Add(20*time.Second), 5*generateSineWaveValue(now.Add(20*time.Second))),
				newSamplePair(now.Add(30*time.Second), 5*generateSineWaveValue(now.Add(30*time.Second))),
			},
			expectedSeries:        5,
			expectedWriteInterval: 10 * time.Second,
			expectedErr:           "",
		},
		"should return error if there's a missing series": {
			samples: []model.SamplePair{
				newSamplePair(now.Add(10*time.Second), 4*generateSineWaveValue(now.Add(10*time.Second))),
				newSamplePair(now.Add(20*time.Second), 4*generateSineWaveValue(now.Add(20*time.Second))),
				newSamplePair(now.Add(30*time.Second), 4*generateSineWaveValue(now.Add(30*time.Second))),
			},
			expectedSeries:        5,
			expectedWriteInterval: 10 * time.Second,
			expectedErr:           "sample at timestamp .* has value .* while was expecting .*",
		},
		"should return error if there's a missing sample": {
			samples: []model.SamplePair{
				newSamplePair(now.Add(10*time.Second), 5*generateSineWaveValue(now.Add(10*time.Second))),
				newSamplePair(now.Add(30*time.Second), 5*generateSineWaveValue(now.Add(30*time.Second))),
			},
			expectedSeries:        5,
			expectedWriteInterval: 10 * time.Second,
			expectedErr:           "sample at timestamp .* was expected to have timestamp .*",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actual := verifySineWaveSamples(testData.samples, testData.expectedSeries, testData.expectedWriteInterval)
			if testData.expectedErr == "" {
				assert.NoError(t, actual)
			} else {
				assert.Error(t, actual)
				assert.Regexp(t, testData.expectedErr, actual.Error())
			}
		})
	}
}

func newSamplePair(ts time.Time, value float64) model.SamplePair {
	return model.SamplePair{
		Timestamp: model.Time(ts.UnixMilli()),
		Value:     model.SampleValue(value),
	}
}
