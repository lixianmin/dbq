package dbq

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

/********************************************************************
created:    2019-11-08
author:     lixianmin

要求MySQL表拥有字段：id, error_message, kind, retry, locked, update_time

CREATE TABLE `notify_queue` (
`id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,
`target_id` bigint(11) NOT NULL DEFAULT '0' COMMENT '用户id',
`error_message` text NOT NULL COMMENT '错误消息',
`kind` int(3) NOT NULL DEFAULT '0' COMMENT '处理类型：1 EmailLow, 2 EmailHigh, 3 SmsLow, 4 SmsHigh',
`retry` int(3) NOT NULL DEFAULT '0' COMMENT '尝试发送次数，含首次发送。如果retry=0则不发送，如果retry=1则只发送一次',
`locked` int(2) NOT NULL DEFAULT '0' COMMENT '是否被锁定',
`create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
`update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='消息队列';

Copyright (C) - All Rights Reserved
 *********************************************************************/

type MySQLQueue struct {
	db   *sql.DB // 数据库连接
	args *MySQLQueueArgs

	selectForLock string
	updateForLock string
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

		selectForLock: fmt.Sprintf("select id from %s where locked = 0 and kind=%d and retry > 0 and now() > update_time limit %d for update;", tableName, kind, concurrency),
		updateForLock: fmt.Sprintf("update %s set locked = 1, retry = retry - 1 where kind=%d and id in (?);", tableName, kind),
		unlockExpires: fmt.Sprintf("update %s set locked = 0 where locked = 1 and kind=%d and retry > 0 and now() > date_add(update_time, interval 60 second) limit 128;", tableName, kind),
		deleteRow:     fmt.Sprintf("delete from %s where id = ?;", tableName),

		// 解锁的时候，利用 update_time 把重试时间设置到 ? seconds 之后
		// 注意：设置了 update_time，不代表一定会重试处理，后者取决于retry字段是否 > 0 。比如retry=3，则nextRetrySeconds()方法最多会被调用3次，
		// 但第3次调用后retry=0，因此虽然设置了下次重试时间，但不会进行重试了。除非手动设置retry字段的值
		unlockRow: fmt.Sprintf("update %s set locked = 0, update_time = date_add(now(), interval ? second) where id = ?;", tableName),
	}

	var rowsChan = make(chan int64, concurrency)
	var retryCountMap = &sync.Map{}

	// 主循环
	go mq.goLoop(tableName, kind, rowsChan)

	// concurrency个任务处理协程
	for i := 0; i < concurrency; i++ {
		go mq.goProcess(listener, rowsChan, retryCountMap)
	}

	return mq
}

func (mq *MySQLQueue) goLoop(tableName string, kind int, rowsChan chan int64) {
	var lockTicker = time.NewTicker(500 * time.Millisecond)
	var unlockTicker = time.NewTicker(5 * time.Minute)
	defer func() {
		lockTicker.Stop()
		unlockTicker.Stop()
	}()

	for {
		select {
		case <-lockTicker.C:
			var rows, err = mq.lockForProcess()
			if err == nil {
				for i := 0; i < len(rows); i++ {
					var rowId = rows[i]
					rowsChan <- rowId
				}
			} else {
				logger.Error(err)
			}
		case <-unlockTicker.C:
			if _, err := mq.db.Exec(mq.unlockExpires); err != nil {
				logger.Error(err)
			}
		}
	}
}

func (mq *MySQLQueue) goProcess(listener IRowListener, rowsChan chan int64, retryCountMap *sync.Map) {
	for {
		select {
		case rowId := <-rowsChan:
			var action = listener.consume(rowId)

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

	var retrySeconds = mq.args.nextRetrySeconds(retryCount)
	if _, err := mq.db.Exec(mq.unlockRow, retrySeconds, rowId); err != nil {
		logger.Error(err)
	}
}

func (mq *MySQLQueue) lockForProcess() ([]int64, error) {
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
	var rowIds []int64
	for rows.Next() {
		var rowId int64
		if err := rows.Scan(&rowId); err != nil {
			return nil, err
		}
		rowIds = append(rowIds, rowId)
	}

	_, err = tx.Exec(mq.updateForLock, rows)
	if err != nil {
		return nil, err
	}

	return rowIds, tx.Commit()
}
