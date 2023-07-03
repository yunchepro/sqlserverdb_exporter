package collector

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"yunche.pro/dtsre/mssql_exporter/dbutil"
)

var (
	mssqlConfigDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "configuration", "value"),
		"MSSQL Configuration Info",
		[]string{"name"}, nil)
)

type ScrapeMSSQLConfig struct {
	lastTime time.Time
}

func (ScrapeMSSQLConfig) Name() string {
	return "mssql_configuration"
}

func (ScrapeMSSQLConfig) Help() string {
	return "collect stats from sys.configurations"

}

func (ScrapeMSSQLConfig) Version() float64 {
	return 10.2
}

func (s *ScrapeMSSQLConfig) Scrape(ctx context.Context, dbcli *dbutil.MSSQLClient, ch chan<- prometheus.Metric, ins *InstanceInfoAll) error {

	sql := `select name, value_in_use from sys.configurations
where name in (
'cost threshold for parallelism',
'cursor threshold',
'fill factor',
'max degree of parallelism',
'max server memory',
'max worker threads',
'recovery interval',
'remote access',
'remote admin connections',
'user connections',
'locks', 
'remote login timeout (s)',
'remote query timeout (s)',
'min server memory (MB)',
'max server memory (MB)'
)
`

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return nil
	}

	for _, r := range rows {

		ch <- prometheus.MustNewConstMetric(
			mssqlConfigDesc, prometheus.GaugeValue, float64(r[1].(int64)), r[0].(string))
	}

	return nil
}
