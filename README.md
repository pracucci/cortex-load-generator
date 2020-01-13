# Cortex Load Generator

Multi-tenant Cortex load generator inspired by [`avalanche`](https://github.com/open-fresh/avalanche) (part of the code has been copied from Avalanche).

```
usage: cortex-load-generator --remote-url=REMOTE-URL [<flags>]

cortex-load-generator

Flags:
  --help                       Show context-sensitive help (also try --help-long and --help-man).
  --remote-url=REMOTE-URL      URL to send samples via remote_write API.
  --remote-write-interval=10s  Frequency to generate new series data points and send them to the remote endpoint.
  --remote-write-timeout=5s    Remote endpoint write timeout.
  --remote-batch-size=1000     how many samples to send with each write request.
  --tenants-count=1            Number of tenants to fake.
  --series-count=1000          Number of series to generate for each tenant.
  --version                    Show application version.
```
