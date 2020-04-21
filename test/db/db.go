package db

import (
	"database/sql"
	"fmt"
	"joe/mq/test/mysql"
)

const (
	MQtestDB = "root:ucloud.cn@tcp(127.0.0.1:3306)/mqtest?charset=utf8"
)

// GetDBConn ...
func GetDBConn(name string) (*sql.DB, error) {
	db, err := mysql.GetConn(MQtestDB)
	if err != nil {
		return nil, fmt.Errorf("mysql.GetConn error %s", err.Error())
	}

	return db, nil
}
