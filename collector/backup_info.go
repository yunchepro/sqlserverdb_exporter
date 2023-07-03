package collector

import (
	"context"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"yunche.pro/dtsre/mssql_exporter/dbutil"
)

var (
	dbBackupColumns = []string{"backup_set_id", "backup_set_uuid", "expiration_date", "name", "user_name",
		"first_lsn", "last_lsn", "checkpoint_lsn", "database_backup_lsn", "database_creation_date",
		"backup_start_date", "backup_finish_date", "type", "database_name", "server_name",
		"machine_name", "recovery_model", "is_damaged", "differential_base_lsn", "differential_base_guid"}

	dbBackupDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "database", "backup"),
		"MSSQL Database Space Info",
		dbBackupColumns, nil)

	intervalDbBackup = 30 * time.Second
)

type ScrapeDbBackup struct {
	lastTime time.Time
}

func (ScrapeDbBackup) Name() string {
	return "mssql_db_backup"
}

func (ScrapeDbBackup) Help() string {
	return "collect stats from msdb.dbo.backupset"
}

func (ScrapeDbBackup) Version() float64 {
	return 10.2
}

func (s *ScrapeDbBackup) Scrape(ctx context.Context, dbcli *dbutil.MSSQLClient, ch chan<- prometheus.Metric, ins *InstanceInfoAll) error {
	duration := time.Since(s.lastTime)
	if duration < intervalDbSpace {
		return nil
	}

	sql := `select backup_set_id, backup_set_uuid, expiration_date, name,  user_name, 
	first_lsn, last_lsn, checkpoint_lsn, database_backup_lsn, database_creation_date, 
	backup_start_date, backup_finish_date, type, database_name, server_name, 
	machine_name, recovery_model, is_damaged, differential_base_lsn, differential_base_guid, 
	backup_size, compressed_backup_size
from msdb.dbo.backupset
where backup_finish_date >= dateadd(minute, -1440000, GETDATE())
`

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return nil
	}

	currentTime := time.Now()

	for _, r := range rows {

		backupSize, err := strconv.ParseFloat(formatNullableByteArray(r[20]), 64)
		if err != nil {
			return err
		}

		ch <- prometheus.MustNewConstMetric(
			dbBackupDesc, prometheus.GaugeValue, backupSize,
			formatInt64(r[0].(int64)),
			formatNullableByteArray(r[1]),
			formatNullableTime(r[2]),
			formatNullableString(r[3]),
			formatNullableString(r[4]),
			formatNullableByteArray(r[5]),
			formatNullableByteArray(r[6]),
			formatNullableByteArray(r[7]),
			formatNullableByteArray(r[8]),
			formatTime(r[9].(time.Time)),
			formatTime(r[10].(time.Time)),
			formatTime(r[11].(time.Time)),
			formatNullableString(r[12]),
			formatNullableString(r[13]),
			formatNullableString(r[14]),
			formatNullableString(r[15]),
			formatNullableString(r[16]),
			formatBool(r[17].(bool)),
			formatNullableByteArray(r[18]),
			formatNullableByteArray(r[19]),
		)
	}

	s.lastTime = currentTime

	return nil
}
