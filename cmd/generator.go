package main

import (
	"fmt"
	"os"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/pracucci/cortex-load-generator/pkg/client"
	"github.com/pracucci/cortex-load-generator/pkg/util"
)

var (
	remoteURL              = kingpin.Flag("remote-url", "URL to send samples via remote_write API.").Required().URL()
	remoteWriteInterval    = kingpin.Flag("remote-write-interval", "Frequency to generate new series data points and send them to the remote endpoint.").Default("10s").Duration()
	remoteWriteTimeout     = kingpin.Flag("remote-write-timeout", "Remote endpoint write timeout.").Default("5s").Duration()
	remoteWriteConcurrency = kingpin.Flag("remote-write-concurrency", "The max number of concurrent batch write requests per tenant.").Default("10").Int()
	remoteBatchSize        = kingpin.Flag("remote-batch-size", "how many samples to send with each write request.").Default("1000").Int()
	queryEnabled           = kingpin.Flag("query-enabled", "True to run queries to assess correctness").Default("false").Enum("true", "false")
	queryURL               = kingpin.Flag("query-url", "Base URL of the query endpoint.").String()
	queryInterval          = kingpin.Flag("query-interval", "Frequency to query each tenant.").Default("10s").Duration()
	queryTimeout           = kingpin.Flag("query-timeout", "Query timeout.").Default("30s").Duration()
	queryMaxAge            = kingpin.Flag("query-max-age", "How back in the past metrics can be queried at most.").Default("24h").Duration()
	additionalQueries      = kingpin.Flag("query-additional-queries", "PromQL queries to run in addition to the default.").Strings()
	tenantsCount           = kingpin.Flag("tenants-count", "Number of tenants to fake.").Default("1").Int()
	seriesCount            = kingpin.Flag("series-count", "Number of series to generate for each tenant.").Default("1000").Int()
	seriesChurnPeriod      = kingpin.Flag("series-churn-period", "How frequently the series should churn. Each series will churn over this duration, and series churning time is spread over the configured period (they will not churn all at the same time). 0 to disable churning.").Default("0").Duration()
	extraLabelCount        = kingpin.Flag("extra-labels-count", "Number of extra labels to generate for series.").Default("0").Int()
	serverMetricsPort      = kingpin.Flag("server-metrics-port", "The port where metrics are exposed.").Default("9900").Int()
	tenantIDPrefix         = kingpin.Flag("tenant-id-prefix", "Tenant ID prefix. The user IDs will be generated as \"<tenant-id-prefix>-<i>\" for every i in [0, tenants-count)").Default("load-generator").String()
)

func main() {
	// Parse CLI flags.
	kingpin.Version("0.0.1")
	kingpin.CommandLine.Help = "cortex-load-generator"
	kingpin.Parse()

	// Run the instrumentation server.
	logger := log.NewLogfmtLogger(os.Stdout)
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())

	i := util.NewInstrumentationServer(*serverMetricsPort, logger, reg)
	if err := i.Start(); err != nil {
		level.Error(logger).Log("msg", "Unable to start instrumentation server", "err", err.Error())
		os.Exit(1)
	}

	// Start a client for each tenant.
	wg := sync.WaitGroup{}
	wg.Add(*tenantsCount)

	for t := 1; t <= *tenantsCount; t++ {
		userID := fmt.Sprintf("%s-%d", *tenantIDPrefix, t)

		writeClient := client.NewWriteClient(client.WriteClientConfig{
			URL:               **remoteURL,
			WriteInterval:     *remoteWriteInterval,
			WriteTimeout:      *remoteWriteTimeout,
			WriteConcurrency:  *remoteWriteConcurrency,
			WriteBatchSize:    *remoteBatchSize,
			UserID:            userID,
			SeriesCount:       *seriesCount,
			SeriesChurnPeriod: *seriesChurnPeriod,
			ExtraLabels:       *extraLabelCount,
		}, logger)

		writeClient.Start()

		if *queryEnabled == "true" {
			queryClient := client.NewQueryClient(client.QueryClientConfig{
				URL:                   *queryURL,
				UserID:                userID,
				QueryInterval:         *queryInterval,
				QueryTimeout:          *queryTimeout,
				QueryMaxAge:           *queryMaxAge,
				ExpectedSeries:        *seriesCount,
				ExpectedWriteInterval: *remoteWriteInterval,
				AdditionalQueries:     *additionalQueries,
			}, logger, reg)

			queryClient.Start()
		}
	}

	// Will wait indefinitely.
	wg.Wait()
}
