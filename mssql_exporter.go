package main

import (
	"os"

	"context"
	"net/http"
	"strconv"
	"time"

	"yunche.pro/dtsre/mssql_exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
	"yunche.pro/dtsre/mssql_exporter/logutil"
)

var (
	metricPath = kingpin.Flag(
		"web.telemetry-path",
		"Path under which to expose metrics.",
	).Default("/metrics").String()

	listenAddress = kingpin.Flag(
		"web.listen-address",
		"Address to listen on for web interface and telemetry.",
	).Default(":9206").String()
	timeoutOffset = kingpin.Flag(
		"timeout-offset",
		"Offset to subtract from timeout in seconds.",
	).Default("0.25").Float64()

	configFile = kingpin.Flag("config", "exporter config file").Default("mssql_exporter.yaml").String()
	loglevel   = kingpin.Flag("level", "exporter log level").Default("info").String()
)

var scrapers = map[collector.Scraper]bool{
	&collector.ScrapeMSSQLInfo{}:        true,
	&collector.ScrapeMSSQLPerfCounter{}: true,
	&collector.ScrapeSQLStat{}:          false,
	&collector.ScrapeWaitStat{}:         true,
	&collector.ScrapeDbSpace{}:          true,
	&collector.ScrapeDbBackup{}:         true,
	&collector.ScrapeDbMeta{}:           true,
	&collector.ScrapeDbSession{}:        true,
	&collector.ScrapeDbMirrorState{}:    true,
	&collector.ScrapeMSSQLConfig{}:      true,
}

func main() {
	// Generate ON/OFF flags for all scrapers.
	scraperFlags := map[collector.Scraper]*bool{}
	for scraper, enabledByDefault := range scrapers {
		defaultOn := "false"
		if enabledByDefault {
			defaultOn = "true"
		}

		f := kingpin.Flag(
			"collect."+scraper.Name(),
			scraper.Help(),
		).Default(defaultOn).Bool()

		scraperFlags[scraper] = f
	}

	kingpin.Parse()

	logutil.InitLog("mssql_exporter.log", *loglevel)
	// landingPage contains the HTML served at '/'.
	// TODO: Make this nicer and more informative.
	var landingPage = []byte(`<html>
<head><title>SQL Server Database exporter</title></head>
<body>
<h1>SQL Server Database exporter</h1>
<p><a href='` + *metricPath + `'>Metrics</a></p>
</body>
</html>
`)

	enabledScrapers := []collector.Scraper{}
	for scraper, enabled := range scraperFlags {
		if *enabled {
			log.WithFields(log.Fields{"scraper": scraper.Name()}).Info("Scraper Enabled")
			enabledScrapers = append(enabledScrapers, scraper)
		}
	}

	handlerFunc := newHandler(collector.NewMetrics(), enabledScrapers)
	log.WithFields(log.Fields{"metricPath": *metricPath}).Debug("handler for metricPath")
	// http.Handle(*metricPath, promhttp.InstrumentMetricHandler(prometheus.DefaultRegisterer, handlerFunc))
	http.Handle(*metricPath, handlerFunc)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write(landingPage)
	})

	log.WithFields(log.Fields{"address": *listenAddress}).Info("Listening on address")
	srv := &http.Server{Addr: *listenAddress}
	if err := srv.ListenAndServe(); err != nil {
		log.WithFields(log.Fields{"err": err}).Error("Error starting HTTP server")
		os.Exit(1)
	}
}

func newHandler(metrics collector.Metrics, scrapers []collector.Scraper) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filteredScrapers := scrapers
		params := r.URL.Query()["collect[]"]
		// Use request context for cancellation when connection gets closed.
		ctx := r.Context()
		// If a timeout is configured via the Prometheus header, add it to the context.
		if v := r.Header.Get("X-Prometheus-Scrape-Timeout-Seconds"); v != "" {
			timeoutSeconds, err := strconv.ParseFloat(v, 64)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("Failed to parse timeout from Prometheus header")
			} else {
				if *timeoutOffset >= timeoutSeconds {
					// Ignore timeout offset if it doesn't leave time to scrape.
					log.WithFields(log.Fields{"offset": *timeoutOffset, "prometheus_scrape_timeout": timeoutSeconds}).Error("Timeout offset should be lower than prometheus scrape timeout")
				} else {
					// Subtract timeout offset from timeout.
					timeoutSeconds -= *timeoutOffset
				}
				// Create new timeout context with request context as parent.
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Duration(timeoutSeconds*float64(time.Second)))
				defer cancel()
				// Overwrite request with timeout context.
				r = r.WithContext(ctx)
			}
		}

		// Check if we have some "collect[]" query parameters.
		if len(params) > 0 {
			filters := make(map[string]bool)
			for _, param := range params {
				filters[param] = true
			}

			filteredScrapers = nil
			for _, scraper := range scrapers {
				if filters[scraper.Name()] {
					filteredScrapers = append(filteredScrapers, scraper)
				}
			}
		}

		registry := prometheus.NewRegistry()
		registry.MustRegister(collector.New(ctx, filteredScrapers, *configFile))

		gatherers := prometheus.Gatherers{
			prometheus.DefaultGatherer,
			registry,
		}
		// Delegate http serving to Prometheus client library, which will call collector.Collect.
		h := promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{})
		h.ServeHTTP(w, r)
	}
}
