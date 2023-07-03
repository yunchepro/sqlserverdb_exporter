package collector

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"yunche.pro/dtsre/mssql_exporter/dbutil"
)

var (
	dbMirrorStateCols = []string{
		"db_name",
		"role",
		"safety_level",
		"partner_name",
		"witness_name",
	}

	dbMirrorStateDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "mirror", "partner_state"),
		"MSSQL Database Mirror State",
		dbMirrorStateCols, nil)

	dbWitnessStateDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "mirror", "witness_state"),
		"MSSQL Database Witness State",
		dbMirrorStateCols, nil)

	intervalDbMirrorState = 5 * time.Second
)

type ScrapeDbMirrorState struct {
	lastTime time.Time
}

func (ScrapeDbMirrorState) Name() string {
	return "mssql_mirror_status"
}

func (ScrapeDbMirrorState) Help() string {
	return "collect stats from sys.database_mirroring"

}

func (ScrapeDbMirrorState) Version() float64 {
	return 10.2
}

func (s *ScrapeDbMirrorState) Scrape(ctx context.Context, dbcli *dbutil.MSSQLClient, ch chan<- prometheus.Metric, ins *InstanceInfoAll) error {
	duration := time.Since(s.lastTime)
	if duration < intervalDbMirrorState {
		return nil
	}

	sql := `select  db_name(database_id), 
	mirroring_role_desc,
	mirroring_safety_level_desc, 
	mirroring_partner_name, 
	mirroring_witness_name,
	mirroring_state,
	mirroring_witness_state
from sys.database_mirroring
where mirroring_partner_name is not null
	`

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return nil
	}

	currentTime := time.Now()

	for _, r := range rows {

		ch <- prometheus.MustNewConstMetric(
			dbMirrorStateDesc, prometheus.GaugeValue, float64(r[5].(int64)),
			r[0].(string),
			r[1].(string),
			r[2].(string),
			r[3].(string),
			formatNullableString(r[4].(string)),
		)

		// witness is not null
		if r[4] != nil {

			ch <- prometheus.MustNewConstMetric(
				dbWitnessStateDesc, prometheus.GaugeValue, float64(r[6].(int64)),
				r[0].(string),
				r[1].(string),
				r[2].(string),
				r[3].(string),
				r[4].(string),
			)
		}

	}

	s.lastTime = currentTime

	return nil
}
