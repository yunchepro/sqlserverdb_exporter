package main

import (
	"context"
	// "database/sql"
	"fmt"
	"time"

	"yunche.pro/dtsre/mssql_exporter/dbutil"
)

var (
	configFile = "mssql.yaml"
)

func main() {
	fmt.Println("test")
	querydb()
}

func querydb() {
	dbcli := dbutil.NewMSSQLClient(configFile)
	err := dbcli.Init()
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	rows, err1 := dbcli.FetchRowsWithContext(context.Background(), "select * from sys.databases")
	if err1 != nil {
		fmt.Println("Fetch error", err1)
		return
	}

	for _, r := range rows {
		fmt.Println("ROW:", r[0].(string), r[1].(int64), nullableInt(r[2]), r[4].(time.Time))
	}
}

func nullableString(p interface{}) string {
	if p == nil {
		return ""
	}
	return p.(string)
}

func nullableInt(p interface{}) int64 {
	if p == nil {
		return 0
	}
	return p.(int64)
}
