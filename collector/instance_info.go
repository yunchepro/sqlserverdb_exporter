package collector

import (
	"context"

	// log "github.com/sirupsen/logrus"
	"yunche.pro/dtsre/mssql_exporter/dbutil"
)

type InstanceInfoAll struct {
	Version                  string
	MachineName              string
	ServerName               string
	InstanceName             string
	ComputerName             string
	Edition                  string
	ProductLevel             string
	ProductionVersion        string
	Collation                string
	IsClustered              int64
	IsFullTextInstalled      int64
	IsIntegratedSecurityOnly int64
	IsHadrEnabled            int64
	HadrManagerStatus        int64
	VersionNum               float64
}

func getInstanceInfo(ctx context.Context, dbcli *dbutil.MSSQLClient) (*InstanceInfoAll, error) {
	sql := `SELECT @@VERSION as version,
SERVERPROPERTY('MachineName') AS [MachineName],
SERVERPROPERTY('ServerName') AS [ServerName],  
SERVERPROPERTY('InstanceName') AS [Instance], 
SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS [ComputerNamePhysicalNetBIOS], 
SERVERPROPERTY('Edition') AS [Edition], 
SERVERPROPERTY('ProductLevel') AS [ProductLevel], 
SERVERPROPERTY('ProductVersion') AS [ProductVersion], 
SERVERPROPERTY('Collation') AS [Collation], 
SERVERPROPERTY('IsClustered') AS [IsClustered], 
SERVERPROPERTY('IsFullTextInstalled') AS [IsFullTextInstalled], 
SERVERPROPERTY('IsIntegratedSecurityOnly') AS [IsIntegratedSecurityOnly],
SERVERPROPERTY('IsHadrEnabled') AS [IsHadrEnabled],
SERVERPROPERTY('HadrManagerStatus') AS [HadrManagerStatus]
`
	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return nil, err
	}

	inst := InstanceInfoAll{}
	r := rows[0]
	inst.Version = r[0].(string)
	inst.MachineName = r[1].(string)
	inst.ServerName = r[2].(string)
	inst.InstanceName = r[3].(string)
	inst.ComputerName = r[4].(string)
	inst.Edition = r[5].(string)
	inst.ProductLevel = r[6].(string)
	inst.ProductionVersion = r[7].(string)
	inst.Collation = r[8].(string)
	inst.IsClustered = r[9].(int64)
	inst.IsFullTextInstalled = r[10].(int64)
	inst.IsIntegratedSecurityOnly = r[11].(int64)
	inst.IsHadrEnabled = r[12].(int64)
	inst.HadrManagerStatus = r[13].(int64)
	inst.VersionNum = 0.0
	return &inst, nil
}
