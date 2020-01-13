package main

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
)

func main() {
	local := ":8081"
	remote := "http://localhost:8080"
	tenantID := "load-generator-1"

	// Parse the remote URL.
	remoteURL, err := url.Parse(remote)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		os.Exit(1)
	}

	// Setup a reverse proxy to inject the tenant ID.
	proxy := httputil.NewSingleHostReverseProxy(remoteURL)
	proxy.Director = func(req *http.Request) {
		req.URL.Host = remoteURL.Host
		req.URL.Scheme = remoteURL.Scheme
		req.Host = remoteURL.Host
		req.Header.Set("X-Scope-OrgID", tenantID)
	}

	// Start a local HTTP server.
	http.HandleFunc("/", proxy.ServeHTTP)
	if err := http.ListenAndServe(local, nil); err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		os.Exit(1)
	}
}
