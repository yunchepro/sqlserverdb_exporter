package collector

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"yunche.pro/dtsre/mssql_exporter/dbutil"
)

var (
	waitStatWaitingTasksDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "waitstat", "waiting_tasks"),
		"MSSQL Instance Info",
		[]string{"wait_type"}, nil)

	waitStatWaitTimeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "waitstat", "wait_time_ms"),
		"MSSQL Instance Info",
		[]string{"wait_type"}, nil)

	waitStatSignalWaitTimeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "waitstat", "signal_wait_time_ms"),
		"MSSQL Instance Info",
		[]string{"wait_type"}, nil)

	intervalWaitStat = 5 * time.Second
)

type ScrapeWaitStat struct {
	lastTime time.Time
}

func (ScrapeWaitStat) Name() string {
	return "mssql_wait_stat"
}

func (ScrapeWaitStat) Help() string {
	return "collect stats from sys.dm_os_wait_stats"

}

func (ScrapeWaitStat) Version() float64 {
	return 10.2
}

func (s *ScrapeWaitStat) Scrape(ctx context.Context, dbcli *dbutil.MSSQLClient, ch chan<- prometheus.Metric, ins *InstanceInfoAll) error {
	duration := time.Since(s.lastTime)
	if duration < intervalWaitStat {
		return nil
	}

	sql := `SELECT wait_type,
    SUM (waiting_tasks_count) AS waiting_tasks_count, 
    SUM (signal_wait_time_ms) AS signal_wait_time_ms, 
    SUM (wait_time_ms) AS wait_time_ms, 
    SUM (raw_wait_time_ms) AS raw_wait_time_ms
FROM 
(
    -- global server wait stats (completed waits only)
    SELECT 
        wait_type, 
        waiting_tasks_count, 
        (wait_time_ms - signal_wait_time_ms) AS wait_time_ms,  
        signal_wait_time_ms, 
        wait_time_ms AS raw_wait_time_ms
    FROM sys.dm_os_wait_stats
    WHERE waiting_tasks_count > 0 OR wait_time_ms > 0 OR signal_wait_time_ms > 0
    UNION ALL 
    -- threads in an in-progress wait (not yet completed waits)
    SELECT 
        wait_type, 
        1 AS waiting_tasks_count, 
        wait_duration_ms AS wait_time_ms, 
        0 AS signal_wait_time_ms, 
        wait_duration_ms AS raw_wait_time_ms
    FROM sys.dm_os_waiting_tasks
    -- Very brief waits quickly will roll into dm_os_wait_stats; we only need to 
    -- query dm_os_waiting_tasks to handle longer-lived waits. 
    WHERE wait_duration_ms > 1000
) AS merged_wait_stats
where wait_type not in (
	'BROKER_EVENTHANDLER',
	'BROKER_RECEIVE_WAITFOR',
	'BROKER_TASK_STOP',
	'BROKER_TO_FLUSH',
	'BROKER_TRANSMITTER',
	'CHECKPOINT_QUEUE',
	'CHKPT',
	'CLR_AUTO_EVENT',
	'CLR_MANUAL_EVENT',
	'CLR_SEMAPHORE',
	'CXCONSUMER',
	'DBMIRROR_DBM_EVENT',
	'DBMIRROR_EVENTS_QUEUE',
	'DBMIRROR_WORKER_QUEUE',
	'DBMIRRORING_CMD',
	'DIRTY_PAGE_POLL',
	'DISPATCHER_QUEUE_SEMAPHORE',
	'EXECSYNC',
	'FSAGENT',
	'FT_IFTS_SCHEDULER_IDLE_WAIT',
	'FT_IFTSHC_MUTEX',
	'HADR_CLUSAPI_CALL',
	'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
	'HADR_LOGCAPTURE_WAIT',
	'HADR_NOTIFICATION_DEQUEUE',
	'HADR_TIMER_TASK',
	'HADR_WORK_QUEUE',
	'KSOURCE_WAKEUP',
	'LAZYWRITER_SLEEP',
	'LOGMGR_QUEUE',
	'MEMORY_ALLOCATION_EXT',
	'ONDEMAND_TASK_QUEUE',
	'PARALLEL_REDO_DRAIN_WORKER',
	'PARALLEL_REDO_LOG_CACHE',
	'PARALLEL_REDO_TRAN_LIST',
	'PARALLEL_REDO_WORKER_SYNC',
	'PARALLEL_REDO_WORKER_WAIT_WORK',
	'PREEMPTIVE_OS_FLUSHFILEBUFFERS',
	'PREEMPTIVE_XE_GETTARGETSTATE',
	'PVS_PREALLOCATE',
	'PWAIT_ALL_COMPONENTS_INITIALIZED',
	'PWAIT_DIRECTLOGCONSUMER_GETNEXT',
	'PWAIT_EXTENSIBILITY_CLEANUP_TASK',
	'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP',
	'QDS_ASYNC_QUEUE',
	'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
	'QDS_SHUTDOWN_QUEUE',
	'REDO_THREAD_PENDING_WORK',
	'REQUEST_FOR_DEADLOCK_SEARCH',
	'RESOURCE_QUEUE',
	'SERVER_IDLE_CHECK',
	'SLEEP_BPOOL_FLUSH',
	'SLEEP_DBSTARTUP',
	'SLEEP_DCOMSTARTUP',
	'SLEEP_MASTERDBREADY',
	'SLEEP_MASTERMDREADY',
	'SLEEP_MASTERUPGRADED',
	'SLEEP_MSDBSTARTUP',
	'SLEEP_SYSTEMTASK',
	'SLEEP_TASK',
	'SLEEP_TEMPDBSTARTUP',
	'SNI_HTTP_ACCEPT',
	'SOS_WORK_DISPATCHER',
	'SP_SERVER_DIAGNOSTICS_SLEEP',
	'SQLTRACE_BUFFER_FLUSH',
	'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
	'SQLTRACE_WAIT_ENTRIES',
	'VDI_CLIENT_OTHER',
	'WAIT_FOR_RESULTS',
	'WAITFOR',
	'WAITFOR_TASKSHUTDOWN',
	'WAIT_XTP_RECOVERY',
	'WAIT_XTP_HOST_WAIT',
	'WAIT_XTP_OFFLINE_CKPT_NEW_LOG',
	'WAIT_XTP_CKPT_CLOSE',
	'XE_DISPATCHER_JOIN',
	'XE_DISPATCHER_WAIT',
	'XE_TIMER_EVENT')
GROUP BY merged_wait_stats.wait_type`

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return nil
	}

	currentTime := time.Now()

	for _, r := range rows {
		waitType := r[0].(string)

		ch <- prometheus.MustNewConstMetric(
			waitStatWaitingTasksDesc, prometheus.CounterValue, float64(r[1].(int64)), waitType)

		ch <- prometheus.MustNewConstMetric(
			waitStatWaitTimeDesc, prometheus.CounterValue, float64(r[2].(int64)), waitType)

		ch <- prometheus.MustNewConstMetric(
			waitStatSignalWaitTimeDesc, prometheus.CounterValue, float64(r[3].(int64)), waitType)

	}

	s.lastTime = currentTime

	return nil
}
