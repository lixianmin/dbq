package dbq

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"
)

/********************************************************************
created:    2019-11-08
author:     lixianmin

要求MySQL表拥有字段：id, error_message, kind, retry, locked, update_time

CREATE TABLE `notify_queue` (
  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,
  `target_id` bigint(11) NOT NULL DEFAULT '0' COMMENT '目标表的row_id',
  `kind` int(3) NOT NULL DEFAULT '0' COMMENT '处理类型：1 EmailLow, 2 EmailHigh, 3 SmsLow, 4 SmsHigh',
  `retry` int(3) NOT NULL DEFAULT '1' COMMENT '尝试发送次数，含首次发送。如果retry=0则不发送，如果retry=1则只发送一次',
  `locked` int(2) NOT NULL DEFAULT '0' COMMENT '是否被锁定',
  `error_message` varchar(512) NOT NULL COMMENT '错误消息',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='消息队列';

Copyright (C) - All Rights Reserved
 *********************************************************************/

type MySQLQueue struct {
	db   *sql.DB // 数据库连接
	args *MySQLQueueArgs

	selectForLock string
	updateForLock string
	extendLife    string
	unlockExpires string
	deleteRow     string
	unlockRow     string
}

// db			数据库连接
// tableName 	数据库表名
// kind int     每一个kind对应一种listener对象，拥有单独的concurrency个处理协程
// listener     消息处理器
// args			默认参数，如果不传，则有默认值
func NewMySQLQueue(db *sql.DB, tableName string, kind int, listener IRowListener, args *MySQLQueueArgs) *MySQLQueue {
	if db == nil || tableName == "" || listener == nil {
		logger.Error("invalid argument, db=%v, tableName=%q, listener=%v", db, tableName, listener)
		return nil
	}

	if args == nil {
		args = &MySQLQueueArgs{}
	}
	args.checkFillDefaultArgs()

	var concurrency = args.concurrency
	var mq = &MySQLQueue{
		db:   db,
		args: args,

		selectForLock: fmt.Sprintf("select id from %s where locked = 0 and kind = %d and retry > 0 and now() > update_time limit %d for update;", tableName, kind, concurrency),
		updateForLock: fmt.Sprintf("update %s set locked = 1, retry = retry - 1 where kind = %d and id in (%%s);", tableName, kind),
		extendLife:    fmt.Sprintf("update %s set update_time = now() where id = ?;", tableName),
		unlockExpires: fmt.Sprintf("update %s set locked = 0 where locked = 1 and kind = %d and retry > 0 and now() > date_add(update_time, interval 60 second) limit 128;", tableName, kind),
		deleteRow:     fmt.Sprintf("delete from %s where id = ?;", tableName),

		// 解锁的时候，利用 update_time 把重试时间设置到 ? seconds 之后
		// 注意：设置了 update_time，不代表一定会重试处理，后者取决于retry字段是否 > 0 。比如retry=3，则nextRetrySeconds()方法最多会被调用3次，
		// 但第3次调用后retry=0，因此虽然设置了下次重试时间，但不会进行重试了。除非手动设置retry字段的值
		unlockRow: fmt.Sprintf("update %s set locked = 0, update_time = date_add(now(), interval ? second) where id = ?;", tableName),
	}

	var rowsChan = make(chan int64, concurrency)
	var processingRows = &sync.Map{}
	var retryCountMap = &sync.Map{}

	// 主循环
	go mq.goLoop(tableName, kind, rowsChan, processingRows)

	// concurrency个任务处理协程
	for i := 0; i < concurrency; i++ {
		go mq.goProcess(listener, rowsChan, processingRows, retryCountMap)
	}

	return mq
}

func (mq *MySQLQueue) goLoop(tableName string, kind int, rowsChan chan int64, processingRows *sync.Map) {
	var lockTicker = time.NewTicker(500 * time.Millisecond)
	var unlockTicker = time.NewTicker(2 * time.Minute)
	var extendLifeTicker = time.NewTicker(30 * time.Second)

	defer func() {
		lockTicker.Stop()
		unlockTicker.Stop()
		extendLifeTicker.Stop()
	}()

	for {
		select {
		case <-lockTicker.C:
			var rows, err = mq.lockForProcess()
			if err == nil {
				for i := 0; i < len(rows); i++ {
					var rowId = rows[i].(int64)
					rowsChan <- rowId
				}
			} else {
				logger.Error(err)
			}
		case <-unlockTicker.C:
			// 某些row在处理过程，进程会意外重启，因此会处于中间状态。这些row在超时后会被强制解锁，从而有机会重新处理
			if _, err := mq.db.Exec(mq.unlockExpires); err != nil {
				logger.Error(err)
			}
		case <-extendLifeTicker.C:
			// 正在处理中的rows，每隔一段时间将拿到的锁续命一下，防止被unlockTicker强制解锁
			processingRows.Range(func(key, value interface{}) bool {
				var rowId = key.(int64)
				if _, err := mq.db.Exec(mq.extendLife, rowId); err != nil {
					logger.Error(err)
				}

				return true
			})
		}
	}
}

func (mq *MySQLQueue) goProcess(listener IRowListener, rowsChan chan int64, processingRows *sync.Map, retryCountMap *sync.Map) {
	for {
		select {
		case rowId := <-rowsChan:
			processingRows.Store(rowId, nil)
			var action = listener.Consume(rowId)
			processingRows.Delete(rowId)

			switch action {
			case CommitMessage:
				mq.onDeleteRow(rowId, retryCountMap)
			case ReconsumeLater:
				mq.onUnlockRow(rowId, retryCountMap)
			}
		}
	}
}

func (mq *MySQLQueue) onDeleteRow(rowId int64, retryCountMap *sync.Map) {
	retryCountMap.Delete(rowId)
	if _, err := mq.db.Exec(mq.deleteRow, rowId); err != nil {
		logger.Error(err)
	}
}

func (mq *MySQLQueue) onUnlockRow(rowId int64, retryCountMap *sync.Map) {
	var lastRetryCount, _ = retryCountMap.LoadOrStore(rowId, 0)
	var retryCount = lastRetryCount.(int) + 1
	retryCountMap.Store(rowId, retryCount)

	// 计算下一次重试的间隔时间
	var retrySeconds = mq.args.nextRetrySeconds(retryCount)
	if _, err := mq.db.Exec(mq.unlockRow, retrySeconds, rowId); err != nil {
		logger.Error(err)
	}
}

func (mq *MySQLQueue) lockForProcess() ([]interface{}, error) {
	var tx, err = mq.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	rows, err := tx.Query(mq.selectForLock)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	// 循环获取rowIds
	var rowIds []interface{}
	for rows.Next() {
		var rowId int64
		if err := rows.Scan(&rowId); err != nil {
			return nil, err
		}
		rowIds = append(rowIds, rowId)
	}

	if len(rowIds) == 0 {
		return rowIds, nil
	}

	var query = fmt.Sprintf(mq.updateForLock, placeholders(len(rowIds)))
	_, err = tx.Exec(query, rowIds...)
	if err != nil {
		return nil, err
	}

	return rowIds, tx.Commit()
}

func placeholders(n int) string {
	var b strings.Builder
	for i := 0; i < n-1; i++ {
		b.WriteString("?,")
	}

	if n > 0 {
		b.WriteString("?")
	}

	return b.String()
}