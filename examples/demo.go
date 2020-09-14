package main

import (
	_ "github.com/go-sql-driver/mysql" // 初始化 mysql 驱动
	"github.com/lixianmin/dbi"
	"github.com/lixianmin/dbq"
	"github.com/lixianmin/dbq/logger"
	"github.com/lixianmin/logo"
	"time"
)

/********************************************************************
created:    2020-07-22
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type DemoListener struct {
}

func (my *DemoListener) Consume(rowId int64) int {
	logger.Info("开始 rowId=%d", rowId)
	time.Sleep(60 * time.Second)
	logger.Info("结束 rowId=%d", rowId)

	return dbq.CommitMessage
}

func main() {
	var conn, err = dbi.Connect("mysql", "root:123456@tcp(127.0.0.1:3306)/test?parseTime=true&loc=Local")
	if err != nil {
		panic(err)
	}

	// 初始化日志
	logo.GetLogger().SetFilterLevel(logo.LevelDebug)
	dbq.Init(logo.GetLogger())

	var listeners = make(map[int]dbq.IRowListener)
	listeners[1] = &DemoListener{}
	listeners[2] = &DemoListener{}

	dbq.NewMySQLQueue(conn.DB, "push_queue", listeners, &dbq.MySQLQueueArgs{
		Concurrency:  len(listeners),
		PollInterval: time.Second * 5,
		LockTimeout:  time.Second * 10,
	})

	// 等待结束
	time.Sleep(time.Hour)
}
