package collector

import (
	"context"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"yunche.pro/dtsre/mssql_exporter/dbutil"
)

var (
	selectedPerfCounters = map[string]bool{
		"Average Wait Time Base":           true,
		"Average Latch Wait Time Base":     true,
		"Active Temp Tables":               true,
		"Temp Tables Creation Rate":        true,
		"Logins/sec":                       true,
		"Connection Reset/sec":             true,
		"Logouts/sec":                      true,
		"User Connections":                 true,
		"Logical Connections":              true,
		"Transactions":                     true,
		"Processes blocked":                true,
		"Full Scans/sec":                   true,
		"Range Scans/sec":                  true,
		"Probe Scans/sec":                  true,
		"Workfiles Created/sec":            true,
		"Worktables Created/sec":           true,
		"Forwarded Records/sec":            true,
		"Index Searches/sec":               true,
		"Page Splits/sec":                  true,
		"Buffer cache hit ratio":           true,
		"Page lookups/sec":                 true,
		"Database pages":                   true,
		"Target pages":                     true,
		"Lazy writes/sec":                  true,
		"Readahead pages/sec":              true,
		"Page reads/sec":                   true,
		"Page writes/sec":                  true,
		"Checkpoint pages/sec":             true,
		"Background writer pages/sec":      true,
		"Page life expectancy":             true,
		"Cache Hit Ratio":                  true,
		"Batch Requests/sec":               true,
		"Forced Parameterizations/sec":     true,
		"Auto-Param Attempts/sec":          true,
		"Failed Auto-Params/sec":           true,
		"SQL Compilations/sec":             true,
		"SQL Re-Compilations/sec":          true,
		"Total Server Memory (KB)":         true,
		"Database Cache Memory (KB)":       true,
		"Free Memory (KB)":                 true,
		"Stolen Server Memory (KB)":        true,
		"Lock Memory (KB)":                 true,
		"Log Pool Memory (KB)":             true,
		"SQL Cache Memory (KB)":            true,
		"Connection Memory (KB)":           true,
		"Optimizer Memory (KB)":            true,
		"Reserved Server Memory (KB)":      true,
		"Memory Grants Outstanding":        true,
		"Memory Grants Pending":            true,
		"Average Wait Time (ms)":           true,
		"Lock Requests/sec":                true,
		"Lock Timeouts/sec":                true,
		"Lock Wait Time (ms)":              true,
		"Lock Waits/sec":                   true,
		"Number of Deadlocks/sec":          true,
		"Average Latch Wait Time (ms)":     true,
		"Latch Waits/sec":                  true,
		"Number of SuperLatches":           true,
		"Total Latch Wait Time (ms)":       true,
		"Active Transactions":              true,
		"Data File(s) Size (KB)":           true,
		"Log Bytes Flushed/sec":            true,
		"Log File(s) Size (KB)":            true,
		"Log File(s) Used Size (KB)":       true,
		"Log Flush Wait Time":              true,
		"Log Flush Waits/sec":              true,
		"Log Flush Write Time (ms)":        true,
		"Log Flushes/sec":                  true,
		"Percent Log Used":                 true,
		"Transactions/sec":                 true,
		"Write Transactions/sec":           true,
		"Free Space in tempdb (KB)":        true,
		"Longest Transaction Running Time": true,
		"Snapshot Transactions":            true,
		"Version Cleanup rate (KB/s)":      true,
		"Version Generation rate (KB/s)":   true,
		"Version Store Size (KB)":          true,
		"Active parallel threads":          true,
		"Active requests":                  true,
		"Blocked tasks":                    true,
		"CPU usage %":                      true,
		"Queued requests":                  true,
		"Reduced memory grants/sec":        true,
		"Requests completed/sec":           true}

	// oracleStatDesc = prometheus.NewDesc(
	// 	prometheus.BuildFQName(namespace, "stat", "stat"),
	// 	"Oracle Stats",
	// 	[]string{"name"}, nil)
)

const (
	PERF_AVERAGE_BULK           = 1073874176
	PERF_LARGE_RAW_FRACTION     = 537003264
	PERF_COUNTER_LARGE_RAWCOUNT = 65792
	PERF_COUNTER_BULK_COUNT     = 272696576
	PERF_LARGE_RAW_BASE         = 1073939712
)

type ScrapeMSSQLPerfCounter struct{}

func (ScrapeMSSQLPerfCounter) Name() string {
	return "mssql_perfcounter"
}

func (ScrapeMSSQLPerfCounter) Help() string {
	return "collect stats from sys.dm_os_performance_counters"

}

type PerfCounter struct {
	ObjectName   string
	CounterName  string
	InstanceName string
	CntrValue    int64
	CntrType     int64
}

func (ScrapeMSSQLPerfCounter) Version() float64 {
	return 10.0
}

func (s ScrapeMSSQLPerfCounter) Scrape(ctx context.Context, dbcli *dbutil.MSSQLClient, ch chan<- prometheus.Metric, ins *InstanceInfoAll) error {
	var err error
	err = s.ScrapeMSSQLPerfCounter(ctx, dbcli, ch, ins)
	if err != nil {
		return err
	}

	return nil
}

func (ScrapeMSSQLPerfCounter) ScrapeMSSQLPerfCounter(ctx context.Context, dbcli *dbutil.MSSQLClient, ch chan<- prometheus.Metric, ins *InstanceInfoAll) error {
	perfCounters, err := getPerfCounters(ctx, dbcli)
	if err != nil {
		return err
	}

	baseCounterMap := getBaseCounters(perfCounters)
	for _, c := range perfCounters {
		// skip other counters
		if _, ok := selectedPerfCounters[c.CounterName]; !ok {
			continue
		}

		var valueType prometheus.ValueType
		var value float64

		value = float64(c.CntrValue)

		switch c.CntrType {
		case PERF_COUNTER_LARGE_RAWCOUNT:
			valueType = prometheus.GaugeValue
		case PERF_COUNTER_BULK_COUNT:
			valueType = prometheus.CounterValue
		case PERF_LARGE_RAW_FRACTION:
			valueType = prometheus.GaugeValue
			fracValue, ok := getFractionValue(c, baseCounterMap)
			if !ok {
				continue
			}
			value = fracValue
		case PERF_LARGE_RAW_BASE:
			valueType = prometheus.CounterValue
		case PERF_AVERAGE_BULK:
			valueType = prometheus.CounterValue

		}

		perfCounterDesc := prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "perfcounter", formatLabel(c.CounterName)),
			"MSSQL Performance Counter",
			[]string{"object_name", "counter_name", "instance_name"}, nil)

		ch <- prometheus.MustNewConstMetric(perfCounterDesc, valueType, value,
			tailOf(c.ObjectName, ":"), c.CounterName, c.InstanceName)
	}
	return nil
}

func getBaseCounters(counters []PerfCounter) map[string]PerfCounter {
	result := make(map[string]PerfCounter)
	for _, c := range counters {
		if c.CntrType == PERF_LARGE_RAW_BASE {
			mapKey := strings.ToLower(c.CounterName + "-" + c.InstanceName)
			result[mapKey] = c
		}
	}
	return result
}

func getPerfCounters(ctx context.Context, dbcli *dbutil.MSSQLClient) ([]PerfCounter, error) {

	sql := `select object_name, counter_name, instance_name, cntr_value, cntr_type 
	from sys.dm_os_performance_counters`

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("Query Error")
		return nil, err
	}

	var result []PerfCounter

	for _, r := range rows {

		objectName := strings.TrimSpace(r[0].(string))
		counterName := strings.TrimSpace(r[1].(string))
		instanceName := strings.TrimSpace(r[2].(string))
		cntrValue := r[3].(int64)
		cntrType := r[4].(int64)

		counter := PerfCounter{ObjectName: objectName,
			CounterName:  counterName,
			InstanceName: instanceName,
			CntrValue:    cntrValue,
			CntrType:     cntrType,
		}
		result = append(result, counter)
	}
	return result, nil
}

func getFractionValue(c PerfCounter, base map[string]PerfCounter) (float64, bool) {
	var baseCounter string
	if c.CounterName == "Average Wait Time (ms)" {
		baseCounter = "Average Wait Time Base"
	} else {
		baseCounter = c.CounterName + " Base"
	}

	key := strings.ToLower(baseCounter + "-" + c.InstanceName)
	if baseCounter, ok := base[key]; ok {
		baseValue := baseCounter.CntrValue
		if baseValue == 0 {
			return 0, true
		}
		return float64(c.CntrValue) / float64(baseValue), true
	}
	return 0, false
}
