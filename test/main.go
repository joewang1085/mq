package main

import (
	"context"
	"database/sql"
	"fmt"
	"joe/mq"
	"joe/mq/test/db"
	"sync"
	"time"

	"github.com/teris-io/shortid"
)

var (
	DROPSQL = fmt.Sprintf(`DROP TABLE IF EXISTS %s`, TableName)
	INITSQL = fmt.Sprintf(`CREATE TABLE %s (
	  msg_id varchar(255) NOT NULL DEFAULT '',
	  create_time int(11) NOT NULL DEFAULT '0',
	  update_time int(11) NOT NULL DEFAULT '0',
	  delete_time int(11) NOT NULL DEFAULT '0',
	  try_times int(11) NOT NULL DEFAULT '0',
	  resource_id varchar(255) DEFAULT '',
	  cpu int(11) NOT NULL DEFAULT '0',
	  memery int(11) NOT NULL DEFAULT '0',
	  disk int(11) NOT NULL DEFAULT '0',
	  othors int(11) NOT NULL DEFAULT '0',
	  state varchar(255) NOT NULL DEFAULT '',
	  PRIMARY KEY (msg_id)
	) ENGINE=InnoDB DEFAULT CHARSET=latin1`, TableName)

	TableName = "my_mq"

	MQ = mq.MQFactory(
		getLBDB,
		TableName,
	)

	once sync.Once
)

type Message struct {
	mq.Message
	ResourceID string
	CPU        uint32
	Memory     uint32
	Disk       uint32
	Othors     uint32
}

func main() {
	fmt.Println("Init")

	InitLocalMQDB()

	go Consumer()

	go Producer()

	c := make(chan int, 0)
	<-c
}

func InitLocalMQDB() {
	once.Do(
		func() {
			for {
				err := MQ.InitLocalDBMQTable([]string{
					DROPSQL,
					INITSQL,
				})
				if err != nil {
					fmt.Println(err)
				} else {
					return
				}

				time.Sleep(5 * time.Second)
			}
		})
}

func Producer() {
	ctx := context.Background()
	// 生产者: CreateLB
	for i := 0; i < 10; i++ {
		go func() {
			// 连接数据库
			db, err := getLBDB()
			if err != nil {
				fmt.Println(err)
				return
			}

			tx, err := db.Begin()
			if err != nil {
				fmt.Println(err)
				return
			}
			succ := false
			defer func() {
				if !succ {
					tx.Rollback()
				}
			}()

			lbID := GenerateID("lb-")
			fmt.Println("Create LB product: ", lbID)
			fmt.Println("Create msg : call mq.PublishMessage")
			query := `INSERT INTO my_mq (msg_id, create_time, update_time, delete_time, try_times, resource_id, cpu, memery, disk, othors, state)
			VALUES
				(?,unix_timestamp(),unix_timestamp(),0,0,?,?,?,?,?,?)`
			msgID := GenerateID("msg-")
			err = MQ.PublishMessage(ctx, tx, query,
				msgID,
				lbID, 2, 4, 100, 0,
				mq.MQCretated,
			)
			if err != nil {
				fmt.Println(err)
				return
			}

			if err = tx.Commit(); err != nil {
				fmt.Println(err)
				return
			}
			succ = true
		}()
	}
}

func Consumer() {
	ctx := context.Background()
	go MQ.Consume(ctx,
		func() {
			query := fmt.Sprintf(`SELECT msg_id,create_time,update_time,delete_time,try_times,resource_id,cpu,memery,disk,othors,state FROM %s WHERE state=? LIMIT ?`, TableName)
			rows, err := MQ.DescribeMessage(ctx,
				query,
				mq.MQCretated,
				100,
			)
			if err != nil {
				fmt.Println(err)
				return
			}
			msgInfos, err := describeMSG(rows)
			if err != nil {
				fmt.Println(err)
				return
			}
			for _, info := range msgInfos {
				func() {
					fmt.Println("send msg :", info)
					fmt.Println("Call bill")
					fmt.Println("Bill succeed")
					fmt.Println("Begin local transaction:")
					// 连接数据库
					db, err := getLBDB()
					if err != nil {
						fmt.Println(err)
						return
					}

					tx, err := db.Begin()
					if err != nil {
						fmt.Println(err)
						return
					}
					succ := false
					defer func() {
						if !succ {
							tx.Rollback()
						}
					}()
					fmt.Println("Commit LB product table")
					fmt.Println("Commit msg  table: call mq.CommitMessage")
					err = MQ.CommitMessage(ctx, tx, info.MSGID)
					if err != nil {
						fmt.Println(err)
						return
					}

					if err = tx.Commit(); err != nil {
						fmt.Println(err)
						return
					}
					succ = true

				}()
			}
		},
		1)
}

func getLBDB() (*sql.DB, error) {
	db, err := db.GetDBConn("mqtest")
	if err != nil {
		return nil, fmt.Errorf("call getApiserverDB error: %s", err.Error())
	}

	return db, nil
}

func describeMSG(rows *sql.Rows) ([]*Message, error) {
	defer rows.Close()

	var infos []*Message
	for rows.Next() {
		var info Message
		if err := rows.Scan(
			&info.MSGID,
			&info.CreateTime,
			&info.UpdateTime,
			&info.DeleteTime,
			&info.TryTimes,
			&info.ResourceID,
			&info.CPU,
			&info.Memory,
			&info.Disk,
			&info.Othors,
			&info.State,
		); err != nil {
			return nil, fmt.Errorf("rows.Scan error: %s", err.Error())
		}
		infos = append(infos, &info)
	}

	return infos, nil
}

func GenerateID(prefix string) string {
	id, _ := shortid.Generate()
	return prefix + "-" + id
}
