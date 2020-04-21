package mq

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type MQState string

const (
	MQCretated MQState = "Created"
	MQDeleted  MQState = "Deleted"
)

// Message 基本属性，继承使用
type Message struct {
	MSGID      string
	CreateTime uint32
	UpdateTime uint32
	DeleteTime uint32
	TryTimes   uint32
	State      MQState
}

// MQ 利用本地表事务实现分布式事务的消息队列
type MQ interface {

	// 创建本地消息表
	InitLocalDBMQTable(query []string) error

	// 发布消息，带上db tx
	PublishMessage(ctx context.Context, tx *sql.Tx, query string, params ...interface{}) error

	// 订阅消息
	DescribeMessage(ctx context.Context, query string, params ...interface{}) (*sql.Rows, error)

	// 回写消息，带上db tx
	CommitMessage(ctx context.Context, tx *sql.Tx, msgID string) error

	// 消费信息
	Consume(ctx context.Context, task func(), interval int)
}

// 接口实现
type mq struct {
	getDBConnFunc func() (*sql.DB, error)
	tableName     string
}

// MQFactory 对外接口
func MQFactory(getDBFunc func() (*sql.DB, error), table string) MQ {
	return &mq{
		getDBConnFunc: getDBFunc, // TODO:怎么确保 该函数的正确性
		tableName:     table,     // // TODO:怎么确保 该参数的正确性
	}
}

func (m *mq) InitLocalDBMQTable(query []string) error {
	if m.tableName == "" {
		return fmt.Errorf("table not defined!")
	}

	db, err := m.getDBConnFunc()
	if err != nil {
		return err
	}

	if db == nil {
		return fmt.Errorf("db not exsits!")
	}

	for _, sql := range query {
		_, err = db.Exec(sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *mq) PublishMessage(ctx context.Context, tx *sql.Tx, query string, params ...interface{}) error {
	// TODO: ctx
	if m.tableName == "" {
		return fmt.Errorf("table not defined!")
	}

	if tx == nil {
		return fmt.Errorf("tx not exsits!")
	}
	_, err := tx.Exec(query, params...)
	if err != nil {
		return err
	}

	return nil
}

func (m *mq) DescribeMessage(ctx context.Context, query string, params ...interface{}) (*sql.Rows, error) {

	// TODO: ctx

	if m.tableName == "" {
		return nil, fmt.Errorf("table not defined!")
	}

	db, err := m.getDBConnFunc()
	if err != nil {
		return nil, err
	}

	if db == nil {
		return nil, fmt.Errorf("db not exsits!")
	}

	// fmt.Println(query, params)
	rows, err := db.QueryContext(ctx, query, params...)

	if err != nil {
		return nil, fmt.Errorf("db.QueryContext errer: %s with sql: %s", err.Error(), query)
	}

	return rows, nil

}

func (m *mq) CommitMessage(ctx context.Context, tx *sql.Tx, msgID string) error {

	// TODO: ctx

	if m.tableName == "" {
		return fmt.Errorf("table not defined!")
	}

	db, err := m.getDBConnFunc()
	if err != nil {
		return err
	}

	if db == nil {
		return fmt.Errorf("db not exsits!")
	}

	query := fmt.Sprintf("update %s set state = ? where msg_id = ?", m.tableName)

	_, err = tx.Exec(query, MQDeleted, msgID)
	if err != nil {
		return err
	}

	return nil

}

// Consume 消费者，回调函数写业务
func (m *mq) Consume(ctx context.Context, task func(), interval int) {

	// TODO: ctx

	for range time.NewTicker(time.Second * time.Duration(interval)).C {
		task()
	}

}
