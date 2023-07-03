package collector

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"yunche.pro/dtsre/mssql_exporter/dbutil"
)

var (
	sqlStatCols = []string{"query_hash", "creation_time", "begin_time", "end_time", "query_text", "db_name",
		"logical_reads", "physical_reads", "elapsed_time", "worker_time", "clr_time", "rows", "executions",
	}

	sqlStatDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "sql", "stat"),
		"MSSQL Instance Info",
		sqlStatCols, nil)

	intervalSQLStat = 30 * time.Second
)

type SQLStat struct {
	QueryHash      string
	CreationTime   string
	ExecutionCount int64
	LogicalReads   int64
	PhysicalReads  int64
	ElapsedTime    int64
	WorkerTime     int64
	ClrTime        int64
	Rows           int64
	SQLText        string
	DBName         string
}

type ScrapeSQLStat struct {
	lastTime time.Time
	sqlmap   map[string]SQLStat
}

func (ScrapeSQLStat) Name() string {
	return "mssql_sql_stat"
}

func (ScrapeSQLStat) Help() string {
	return "collect sql executions statistics from sys.dm_exec_query_stats"

}

func (ScrapeSQLStat) Version() float64 {
	return 10.2
}

func (s *ScrapeSQLStat) Scrape(ctx context.Context, dbcli *dbutil.MSSQLClient, ch chan<- prometheus.Metric, ins *InstanceInfoAll) error {
	duration := time.Since(s.lastTime)
	if duration < intervalSQLStat {
		return nil
	}

	sqls, err := getTopSql(ctx, dbcli)
	if err != nil {
		return nil
	}
	currentTime := time.Now()

	if s.sqlmap == nil {
		s.lastTime = currentTime
		s.sqlmap = buildSQLMap(sqls)
		return nil
	}

	for _, c := range sqls {
		var sqlStat SQLStat
		hkey := getQueryDigest(c)
		prevSQL, ok := s.sqlmap[hkey]
		if ok {
			sqlStat = getSQLStatDiff(c, prevSQL)
		} else {
			sqlStat = c
		}

		if sqlStat.ExecutionCount > 0 {
			ch <- prometheus.MustNewConstMetric(
				sqlStatDesc, prometheus.GaugeValue, 1,
				c.QueryHash, c.CreationTime, formatTime(s.lastTime), formatTime(currentTime), c.SQLText, c.DBName,
				formatInt64(sqlStat.LogicalReads),
				formatInt64(sqlStat.PhysicalReads),
				formatInt64(sqlStat.ElapsedTime),
				formatInt64(sqlStat.WorkerTime),
				formatInt64(sqlStat.ClrTime),
				formatInt64(sqlStat.Rows),
				formatInt64(sqlStat.ExecutionCount),
			)
		}
	}

	s.lastTime = currentTime
	s.sqlmap = buildSQLMap(sqls)

	return nil
}

func getSQLStatDiff(current SQLStat, prev SQLStat) SQLStat {
	result := SQLStat{}
	result.LogicalReads = current.LogicalReads - prev.LogicalReads
	result.PhysicalReads = current.PhysicalReads - prev.PhysicalReads
	result.ElapsedTime = current.ElapsedTime - prev.ElapsedTime
	result.WorkerTime = current.WorkerTime - prev.WorkerTime
	result.ClrTime = current.ClrTime - prev.ClrTime
	result.Rows = current.Rows - prev.Rows
	result.ExecutionCount = current.ExecutionCount - prev.ExecutionCount

	return result
}

func getTopSql(ctx context.Context, dbcli *dbutil.MSSQLClient) ([]SQLStat, error) {
	sql := `SELECT TOP 500 query_hash, creation_time,
  qs.execution_count,
  (qs.total_logical_reads + qs.total_logical_writes) as total_logical_reads,
  qs.total_physical_reads,
  qs.total_elapsed_time,
  qs.total_worker_time,
  qs.total_clr_time,
  qs.total_rows,
  SUBSTRING (qt.text,(qs.statement_start_offset/2) + 1,  
    ((CASE WHEN qs.statement_end_offset = -1 
    THEN LEN(CONVERT(NVARCHAR(MAX), qt.text)) * 2
	ELSE qs.statement_end_offset
	END - qs.statement_start_offset)/2) + 1) AS query_text,
  ISNULL(DB_NAME(qt.dbid), '') AS DatabaseName
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) as qt
`
	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return nil, err
	}

	var result []SQLStat
	for _, r := range rows {
		sqlstat := SQLStat{
			QueryHash:      formatNullableByteArray(r[0].([]uint8)),
			CreationTime:   formatTime(r[1].(time.Time)),
			ExecutionCount: r[2].(int64),
			LogicalReads:   r[3].(int64),
			PhysicalReads:  r[4].(int64),
			ElapsedTime:    r[5].(int64),
			WorkerTime:     r[6].(int64),
			ClrTime:        r[7].(int64),
			Rows:           r[8].(int64),
			SQLText:        r[9].(string),
			DBName:         r[10].(string),
		}

		result = append(result, sqlstat)
	}
	return result, nil
}

func buildSQLMap(sqls []SQLStat) map[string]SQLStat {
	result := make(map[string]SQLStat)
	for _, s := range sqls {
		hkey := getQueryDigest(s)
		result[hkey] = s
	}
	return result
}

func getQueryDigest(s SQLStat) string {
	return s.SQLText + "/" + s.QueryHash + "/" + s.CreationTime
}
