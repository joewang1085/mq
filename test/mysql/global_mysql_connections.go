package mysql

import (
	"database/sql"
	"sync"
)

var (
	mu         = sync.Mutex{} //open db 操作锁
	mysqlConns = sync.Map{}   //连接锁
)

// GetConn returns the mysql connection for a given data source name.
func GetConn(dataSourceName string) (conn *sql.DB, err error) {
	_conn, ok := mysqlConns.Load(dataSourceName)

	if !ok || _conn == nil {
		conn, err = openDB(dataSourceName)
		if err != nil {
			return nil, err
		}
	} else {
		conn = _conn.(*sql.DB)
	}

	// Ping verifies a connection to the database is still alive, establishing a connection if necessary.
	// ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	// defer cancel()
	// if err = conn.PingContext(ctx); err != nil {
	// 	//这里记录日志,理论不会进入或者不会频繁进入
	// 	logs.Error("Ping mysql: %s", err)
	// }

	return
}

// 锁操作
func openDB(dataSourceName string) (conn *sql.DB, err error) {

	mu.Lock()
	defer mu.Unlock()

	// 二次读取
	if _conn, ok := mysqlConns.Load(dataSourceName); ok {
		return _conn.(*sql.DB), nil
	}

	conn, err = sql.Open("mysql", dataSourceName)
	if err != nil {
		// logs.Error("Open mysql: %s error", dataSourceName, err)
		return nil, err
	}

	conn.SetMaxOpenConns(300)
	conn.SetMaxIdleConns(10)
	mysqlConns.Store(dataSourceName, conn)
	return
}
