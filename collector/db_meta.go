package collector

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"yunche.pro/dtsre/mssql_exporter/dbutil"
)

var (
	dbMetaCols = []string{"name", "database_id", "create_date", "compatibility_level", "collation_name", "recovery_model",
		"snapshot_isolation", "read_committed_snapshot"}
	dbMetaDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "database", "meta"),
		"MSSQL Database Space Info",
		dbMetaCols, nil)
)

type ScrapeDbMeta struct {
	lastTime time.Time
}

func (ScrapeDbMeta) Name() string {
	return "mssql_db_meta"
}

func (ScrapeDbMeta) Help() string {
	return "collect stats from sys.databases"

}

func (ScrapeDbMeta) Version() float64 {
	return 10.2
}

func (s *ScrapeDbMeta) Scrape(ctx context.Context, dbcli *dbutil.MSSQLClient, ch chan<- prometheus.Metric, ins *InstanceInfoAll) error {

	sql := `select name, database_id, create_date, compatibility_level, collation_name, recovery_model_desc, 
	snapshot_isolation_state, is_read_committed_snapshot_on, state, user_access
from sys.databases`

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return nil
	}

	for _, r := range rows {

		ch <- prometheus.MustNewConstMetric(
			dbMetaDesc, prometheus.GaugeValue, float64(r[8].(int64)),
			r[0].(string),
			formatInt64(r[1].(int64)),
			formatTime(r[2].(time.Time)),
			formatInt64((r[3].(int64))),
			r[4].(string),
			r[5].(string),
			formatInt64(r[6].(int64)),
			formatBool(r[7].(bool)),
		)

	}

	return nil
}
