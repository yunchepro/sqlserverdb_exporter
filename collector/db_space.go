package collector

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"yunche.pro/dtsre/mssql_exporter/dbutil"
)

var (
	dbSpaceDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "database", "space"),
		"MSSQL Database Space Info",
		[]string{"db_name", "mode"}, nil)

	intervalDbSpace = 30 * time.Second
)

type ScrapeDbSpace struct {
	lastTime time.Time
}

func (ScrapeDbSpace) Name() string {
	return "mssql_db_space"
}

func (ScrapeDbSpace) Help() string {
	return "collect stats from sys.master_files"

}

func (ScrapeDbSpace) Version() float64 {
	return 10.2
}

func (s *ScrapeDbSpace) Scrape(ctx context.Context, dbcli *dbutil.MSSQLClient, ch chan<- prometheus.Metric, ins *InstanceInfoAll) error {
	duration := time.Since(s.lastTime)
	if duration < intervalDbSpace {
		return nil
	}

	sql := `select DB_NAME(database_id), 
     SUM(case when type=0 then cast (size as bigint) else 0 end) * 8192  data_size, 
     sum(case when type=1 then cast (size as bigint) else 0 end) * 8192  log_size,
	 SUM(case when type > 1 then cast (size as bigint) else 0 end) * 8192 other_size
 from sys.master_files
 group by DB_NAME(database_id)`

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return nil
	}

	currentTime := time.Now()

	for _, r := range rows {
		dbName := r[0].(string)

		ch <- prometheus.MustNewConstMetric(
			dbSpaceDesc, prometheus.GaugeValue, float64(r[1].(int64)), dbName, "data")

		ch <- prometheus.MustNewConstMetric(
			dbSpaceDesc, prometheus.GaugeValue, float64(r[2].(int64)), dbName, "log")

		ch <- prometheus.MustNewConstMetric(
			dbSpaceDesc, prometheus.GaugeValue, float64(r[3].(int64)), dbName, "other")
	}

	s.lastTime = currentTime

	return nil
}
