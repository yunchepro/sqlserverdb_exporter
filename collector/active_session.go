package collector

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"yunche.pro/dtsre/mssql_exporter/dbutil"
)

var (
	dbActiveSessionCols = []string{
		"session_id",
		"client_net_address",
		"client_tcp_port",
		"login_time",
		"login_name",
		"host_name",
		"program_name",
		"status",
		"open_transaction_count",
		"transaction_isolation_level",
		"start_time",
		"command",
		"request_status",
		"wait_type",
		"text",
	}
	dbActiveSessionDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "session", "active"),
		"MSSQL Database Space Info",
		dbActiveSessionCols, nil)

	dbBlockedSessionCols = []string{
		"blocking_session_id",
		"blocking_user",
		"blocking_login_time",
		"blocking_host_name",
		"blocking_program_name",
		"blocking_sql",
		"blocked_session_id",
		"blocked_user",
		"blocked_sql",
		"blocked_db_name",
		"wait_type",
	}
	dbBlockedSessionDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "session", "blocked"),
		"MSSQL Database Space Info",
		dbBlockedSessionCols, nil)
)

type ScrapeDbSession struct {
	lastTime time.Time
}

func (ScrapeDbSession) Name() string {
	return "mssql_active_session"
}

func (ScrapeDbSession) Help() string {
	return "collect active session info"

}

func (ScrapeDbSession) Version() float64 {
	return 10.2
}

func (s *ScrapeDbSession) Scrape(ctx context.Context, dbcli *dbutil.MSSQLClient, ch chan<- prometheus.Metric, ins *InstanceInfoAll) error {
	var err error
	err = s.scrapeActiveSession(ctx, dbcli, ch, ins)
	if err != nil {
		return err
	}

	err = s.scrapeBlockedSession(ctx, dbcli, ch, ins)
	if err != nil {
		return err
	}
	return nil

}

func (s *ScrapeDbSession) scrapeActiveSession(ctx context.Context, dbcli *dbutil.MSSQLClient, ch chan<- prometheus.Metric, ins *InstanceInfoAll) error {
	sql := `select /* mssql_exporter */ a.session_id, 
    a.client_net_address,
	a.client_tcp_port, 
	b.login_time, 
	b.login_name,
    b.host_name, 
	b.program_name, 
	b.status, 
	b.open_transaction_count, 
	b.transaction_isolation_level,
	c.start_time,
	c.command, 
	c.status as request_status, 
	c.wait_type,
	s.text,
	c.total_elapsed_time
from sys.dm_exec_connections a 
	cross apply sys.dm_exec_sql_text (a.most_recent_sql_handle) s, 
	sys.dm_exec_sessions b 
	left join sys.dm_exec_requests c 
on b.session_id = c.session_id 
where a.session_id = b.session_id
and b.session_id > 50
and b.status != 'sleeping'
and s.text not like 'select /* mssql_exporter%'
`
	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return err
	}

	for _, r := range rows {
		ch <- prometheus.MustNewConstMetric(dbActiveSessionDesc, prometheus.GaugeValue, float64(r[15].(int64)),
			formatInt64(r[0].(int64)),
			r[1].(string),
			formatInt64(r[2].(int64)),
			formatNullableTime(r[3]),
			r[4].(string),
			r[5].(string),
			r[6].(string),
			r[7].(string),
			formatInt64(r[8].(int64)),
			formatInt64(r[9].(int64)),
			formatNullableTime(r[10]),
			r[11].(string),
			r[12].(string),
			formatNullableString(r[13]),
			r[14].(string),
		)
	}
	return nil
}

func (s *ScrapeDbSession) scrapeBlockedSession(ctx context.Context, dbcli *dbutil.MSSQLClient, ch chan<- prometheus.Metric, ins *InstanceInfoAll) error {
	sql := `SELECT
	Blocking.session_id as BlockingSessionId,
	Sess.login_name AS BlockingUser,
	Sess.login_time,
	sess.host_name,
	sess.program_name,
	BlockingSQL.text AS BlockingSQL,
	Blocked.session_id AS BlockedSessionId,
	USER_NAME(Blocked.user_id) AS BlockedUser,
	BlockedSQL.text AS BlockedSQL,
	DB_NAME(Blocked.database_id) AS DatabaseName,
	Waits.wait_type WhyBlocked,
	Waits.wait_duration_ms
FROM sys.dm_exec_connections AS Blocking
	INNER JOIN sys.dm_exec_requests AS Blocked ON Blocking.session_id = Blocked.blocking_session_id
	INNER JOIN sys.dm_os_waiting_tasks AS Waits ON Blocked.session_id = Waits.session_id
	RIGHT OUTER JOIN sys.dm_exec_sessions Sess ON Blocking.session_id = sess.session_id
	CROSS APPLY sys.dm_exec_sql_text(Blocking.most_recent_sql_handle) AS BlockingSQL
	CROSS APPLY sys.dm_exec_sql_text(Blocked.sql_handle) AS BlockedSQL
`
	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return err
	}

	for _, r := range rows {
		ch <- prometheus.MustNewConstMetric(dbBlockedSessionDesc, prometheus.GaugeValue, float64(r[11].(int64)),
			formatInt64(r[0].(int64)),
			r[1].(string),
			formatNullableTime(r[2]),
			r[3].(string),
			r[4].(string),
			r[5].(string),
			formatInt64(r[6].(int64)),
			r[7].(string),
			r[8].(string),
			r[9].(string),
			r[10].(string),
		)
	}
	return nil
}
