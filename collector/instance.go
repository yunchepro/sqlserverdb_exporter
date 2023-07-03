package collector

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"yunche.pro/dtsre/mssql_exporter/dbutil"
)

// instance info
// uptime
// db status, instance status
var (
	instanceInfoCols = []string{"version", "machine_name", "server_name",
		"instance_name", "computer_name", "edition", "production_level", "product_version",
		"collation", "is_clustered", "is_fulltext_installed", "is_integrated_security_only",
		"is_hadr_enabled", "hadr_manager_status"}

	mssqlInfoDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "instance", "info"),
		"MSSQL Instance Info",
		instanceInfoCols, nil)
)

type ScrapeMSSQLInfo struct{}

func (ScrapeMSSQLInfo) Name() string {
	return "mssql_instance_info"
}

func (ScrapeMSSQLInfo) Help() string {
	return "collect SQL Server Basic Instance Info"

}

func (ScrapeMSSQLInfo) Version() float64 {
	return 10.2
}

func (ScrapeMSSQLInfo) Scrape(ctx context.Context, dbcli *dbutil.MSSQLClient, ch chan<- prometheus.Metric, ins *InstanceInfoAll) error {

	ch <- prometheus.MustNewConstMetric(
		mssqlInfoDesc, prometheus.GaugeValue, 1,
		ins.Version, ins.MachineName, ins.ServerName, ins.InstanceName,
		ins.ComputerName, ins.Edition, ins.ProductLevel, ins.ProductionVersion,
		ins.Collation, formatInt64(ins.IsClustered),
		formatInt64(ins.IsFullTextInstalled), formatInt64(ins.IsIntegratedSecurityOnly),
		formatInt64(ins.IsHadrEnabled), formatInt64(ins.HadrManagerStatus),
	)

	return nil
}
