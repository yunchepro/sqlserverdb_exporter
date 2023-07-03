package dbutil

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"

	log "github.com/sirupsen/logrus"

	// _ "github.com/denisenkom/go-mssqldb"
	_ "github.com/microsoft/go-mssqldb"
	"gopkg.in/yaml.v2"
)

type MSSQLConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	Instance string
}

type MSSQLClient struct {
	configFile string
	C          MSSQLConfig
	dbconn     *sql.DB
}

type Row []interface{}

func NewMSSQLClient(configFile string) *MSSQLClient {

	cli := MSSQLClient{configFile: configFile}

	return &cli

}

func (c *MSSQLClient) Init() error {
	err := c.initConfig()
	if err != nil {
		return err
	}

	err = c.initConnection()
	if err != nil {
		return err
	}

	return err
}

func (c *MSSQLClient) CloseConnection() error {
	if c.dbconn != nil {
		err := c.dbconn.Close()
		return err
	}
	return nil
}

func (c *MSSQLClient) initConfig() error {
	buf, err := ioutil.ReadFile(c.configFile)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(buf, &c.C)
	return err
}

func (c *MSSQLClient) initConnection() error {
	conn, err := c.Connect()
	if err != nil {
		return err
	}

	err = conn.Ping()
	if err != nil {
		return err
	}

	c.dbconn = conn
	return nil
}

func (c *MSSQLClient) Connect() (*sql.DB, error) {
	// fmt.Printf("\nget new Connect\n")
	dsn := c.getDatasource()
	log.WithFields(log.Fields{"dsn": dsn}).Info("Connect to MSSQL")

	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	return db, nil
}

func (c *MSSQLClient) getDatasource() string {
	// sqlserver://user:pass@hostname/instance?database=test1

	var dsn string
	if c.C.Instance != "" {
		dsn = fmt.Sprintf("sqlserver://%s:%s@%s/%s?database=master&encrypt=disable", c.C.Username, c.C.Password, c.C.Host, c.C.Instance)
	} else {
		dsn = fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=master&encrypt=disable", c.C.Username, c.C.Password, c.C.Host, c.C.Port)

	}

	return dsn
}

func (c *MSSQLClient) ExecuteQuery(querytext string, params ...interface{}) (*sql.Rows, error) {
	ctx := context.Background()
	return c.ExecuteQueryWithContext(ctx, querytext, params...)
}

func (c *MSSQLClient) FetchRowsWithContext(ctx context.Context, querytext string, params ...interface{}) ([]Row, error) {
	rs, err := c.ExecuteQueryWithContext(ctx, querytext, params...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	rows, err := fetchRows(rs)
	return rows, err
}

func (c *MSSQLClient) ExecuteQueryWithContext(ctx context.Context, querytext string, params ...interface{}) (*sql.Rows, error) {
	if c.dbconn == nil {
		return nil, fmt.Errorf("DB Connection is Nil")
	}

	rows, err := c.dbconn.QueryContext(ctx, querytext, params...)
	if err != nil {
		log.WithFields(log.Fields{"error": err, "query": querytext}).Warn("Execute Query")
	}
	return rows, err

}

func fetchRows(rows *sql.Rows) ([]Row, error) {
	var ret []Row

	columnTypes, _ := rows.ColumnTypes()
	// for _, col_type := range columnTypes {
	// 	fmt.Printf("Column Types:%s %v\n", col_type.Name(), col_type.ScanType())
	// }

	var n []interface{}
	for ii := 0; ii < len(columnTypes); ii++ {
		n = append(n, getField(columnTypes[ii].DatabaseTypeName()))
	}

	for rows.Next() {
		var r Row

		err := rows.Scan(n...)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("Scan Row Error")
			return nil, err
		}
		for i, _ := range columnTypes {
			log.WithFields(log.Fields{"column index": i,
				"column_name": columnTypes[i].Name(),
				"scan type":   columnTypes[i].ScanType(),
				"db type":     columnTypes[i].DatabaseTypeName()}).Debug("Col Info")
			vv := getFieldValue(n[i], columnTypes[i].DatabaseTypeName())
			log.WithFields(log.Fields{"column": columnTypes[i].Name(), "value": vv}).Debug("Got Field")

			r = append(r, vv)
		}
		ret = append(ret, r)
	}
	return ret, nil
}

func DumpRows(rows *sql.Rows) {
	columnTypes, _ := rows.ColumnTypes()
	for _, col_type := range columnTypes {
		fmt.Printf("Column Types:%s %v\n", col_type.Name(), col_type.ScanType())
	}

	var n []interface{}
	for ii := 0; ii < len(columnTypes); ii++ {
		// var v interface{}

		n = append(n, getField(columnTypes[ii].DatabaseTypeName()))
	}

	for rows.Next() {
		err := rows.Scan(n...)
		if err != nil {
			fmt.Printf("Scan error: %s", err)
			return
		}
		for i, _ := range columnTypes {
			fmt.Printf("Col:%d %s %s %s,  ", i, columnTypes[i].Name(), columnTypes[i].ScanType(), columnTypes[i].DatabaseTypeName())
			vv := getFieldValue(n[i], columnTypes[i].DatabaseTypeName())
			fmt.Printf("Got Row C: %v\n", vv)

		}
	}
}

func getField(typename string) interface{} {
	// switch typename {
	// case "NCHAR":
	// 	return new(sql.NullString)
	// case "NVARCHAR":
	// 	return new(sql.NullString)
	// case "VARBINARY":
	// 	return new([]uint8)
	// case "VARCHAR":
	// 	return new(sql.NullString)
	// case "CHAR":
	// 	return new(sql.NullString)
	// case "CLOB":
	// 	return new(sql.NullString)
	// case "INT":
	// 	return new(sql.NullInt64)
	// case "BIGINT":
	// 	return new(sql.NullInt64)
	// case "BIT":
	// 	return new(sql.NullBool)
	// case "DATETIME":
	// 	return new(sql.NullTime)
	// case "TIMESTAMP":
	// 	return new(sql.NullTime)
	// case "DATE":
	// 	return new(sql.NullTime)
	// }
	return new(interface{})
}

func getFieldValue(val interface{}, typename string) interface{} {
	// switch typename {
	// case "NCHAR":
	// 	return toString(*val.(*sql.NullString))
	// case "NVARCHAR":
	// 	return toString(*val.(*sql.NullString))
	// case "VARCHAR":
	// 	return *val.(*sql.NullString)
	// case "CHAR":
	// 	return *val.(*sql.NullString)
	// case "CLOB":
	// 	return *val.(*sql.NullString)
	// case "INT":
	// 	return *val.(*sql.NullInt64)
	// case "BIGINT":
	// 	return *val.(*sql.NullInt64)
	// case "BIT":
	// 	return *val.(*sql.NullBool)
	// case "VARBINARY":
	// 	return fmt.Sprintf("%x", *val.(*[]uint8))
	// case "DATETIME":
	// 	return *val.(*sql.NullTime)
	// case "TIMESTAMP":
	// 	return *val.(*sql.NullTime)
	// case "DATE":
	// 	return *val.(*sql.NullTime)
	// }
	return *val.(*interface{})
}

func toString(p sql.NullString) string {
	if p.Valid {
		return p.String
	} else {
		return ""
	}
}
