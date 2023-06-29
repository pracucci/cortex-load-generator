package client

import (
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAlignTimestampToInterval(t *testing.T) {
	assert.Equal(t, time.Unix(30, 0), alignTimestampToInterval(time.Unix(30, 0), 10*time.Second))
	assert.Equal(t, time.Unix(30, 0), alignTimestampToInterval(time.Unix(31, 0), 10*time.Second))
	assert.Equal(t, time.Unix(30, 0), alignTimestampToInterval(time.Unix(39, 0), 10*time.Second))
	assert.Equal(t, time.Unix(40, 0), alignTimestampToInterval(time.Unix(40, 0), 10*time.Second))
}

func TestGenerateSineWaveSeries_WithChurningSeries(t *testing.T) {
	const (
		numSeries   = 3
		churnPeriod = time.Minute
	)

	assertGeneratedSeries := func(t *testing.T, ts time.Time, churnIDs ...string) {
		expected := make([]*prompb.TimeSeries, 0, len(churnIDs))
		for idx, churnID := range churnIDs {
			expected = append(expected, &prompb.TimeSeries{
				Labels:  []*prompb.Label{{Name: "__name__", Value: "cortex_load_generator_sine_wave"}, {Name: "churn", Value: churnID}, {Name: "wave", Value: strconv.Itoa(idx + 1)}},
				Samples: []prompb.Sample{{Timestamp: ts.UnixMilli(), Value: generateSineWaveValue(ts)}},
			})
		}

		assert.Equal(t, expected, generateSineWaveSeries(ts, numSeries, 0, churnPeriod))
	}

	ts, err := time.Parse(time.RFC3339, "2023-06-29T00:00:00Z")
	require.NoError(t, err)

	assertGeneratedSeries(t, ts, "28133280", "28133280", "28133281")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133280", "28133280", "28133281")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133280", "28133281", "28133281")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133280", "28133281", "28133281")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133281", "28133281", "28133281")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133281", "28133281", "28133281")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133281", "28133281", "28133282")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133281", "28133281", "28133282")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133281", "28133282", "28133282")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133281", "28133282", "28133282")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133282", "28133282", "28133282")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133282", "28133282", "28133282")
}

func TestGenerateSineWaveSeries_WithoutChurningSeries(t *testing.T) {
	const (
		numSeries   = 3
		churnPeriod = 0
	)

	assertGeneratedSeries := func(t *testing.T, ts time.Time) {
		expected := make([]*prompb.TimeSeries, 0, numSeries)
		for seriesID := 1; seriesID <= numSeries; seriesID++ {
			expected = append(expected, &prompb.TimeSeries{
				Labels:  []*prompb.Label{{Name: "__name__", Value: "cortex_load_generator_sine_wave"}, {Name: "wave", Value: strconv.Itoa(seriesID)}},
				Samples: []prompb.Sample{{Timestamp: ts.UnixMilli(), Value: generateSineWaveValue(ts)}},
			})
		}

		assert.Equal(t, expected, generateSineWaveSeries(ts, numSeries, 0, churnPeriod))
	}

	ts, err := time.Parse(time.RFC3339, "2023-06-29T00:00:00Z")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		assertGeneratedSeries(t, ts)
		ts = ts.Add(10 * time.Second)
	}
}
