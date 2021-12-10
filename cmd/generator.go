package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/pracucci/cortex-load-generator/pkg/client"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	remoteURL              = kingpin.Flag("remote-url", "URL to send samples via remote_write API.").Required().URL()
	remoteWriteInterval    = kingpin.Flag("remote-write-interval", "Frequency to generate new series data points and send them to the remote endpoint.").Default("10s").Duration()
	remoteWriteTimeout     = kingpin.Flag("remote-write-timeout", "Remote endpoint write timeout.").Default("5s").Duration()
	remoteWriteConcurrency = kingpin.Flag("remote-write-concurrency", "The max number of concurrent batch write requests per tenant.").Default("10").Int()
	remoteBatchSize        = kingpin.Flag("remote-batch-size", "how many samples to send with each write request.").Default("1000").Int()
	tenantsCount           = kingpin.Flag("tenants-count", "Number of tenants to fake.").Default("1").Int()
	seriesCount            = kingpin.Flag("series-count", "Number of series to generate for each tenant.").Default("1000").Int()
)

func main() {
	// Parse CLI flags.
	kingpin.Version("0.0.1")
	log.SetFlags(log.Ltime | log.Lshortfile) // Show file name and line in logs.
	kingpin.CommandLine.Help = "cortex-load-generator"
	kingpin.Parse()

	// Start a client for each tenant.
	clients := make([]*client.WriteClient, 0, *tenantsCount)
	wg := sync.WaitGroup{}
	wg.Add(*tenantsCount)

	for t := 1; t <= *tenantsCount; t++ {
		clients = append(clients, client.NewWriteClient(client.WriteClientConfig{
			URL:              **remoteURL,
			WriteInterval:    *remoteWriteInterval,
			WriteTimeout:     *remoteWriteTimeout,
			WriteConcurrency: *remoteWriteConcurrency,
			WriteBatchSize:   *remoteBatchSize,
			UserID:           fmt.Sprintf("load-generator-%d", t),
			SeriesCount:      *seriesCount,
		}))
	}

	// Will wait indefinitely.
	wg.Wait()
}
