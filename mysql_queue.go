package dbq

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/lixianmin/dbi"
	"github.com/lixianmin/dbq/logger"
	"github.com/lixianmin/got/loom"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

/********************************************************************
created:    2019-11-08
author:     lixianmin

要求MySQL表拥有字段：id, topic, retry, locked, error_message, update_time

CREATE TABLE `notify_queue` (
  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,
  `target_id` bigint(11) NOT NULL DEFAULT '0' COMMENT '目标表的row_id',
  `topic` int(3) NOT NULL DEFAULT '0' COMMENT '消息队列主题：1 EmailLow, 2 EmailHigh, 3 SmsLow, 4 SmsHigh',
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
	db   *dbi.DB // 数据库连接
	args *MySQLQueueArgs

	selectForLock  string
	updateForLock  string
	extendLife     string
	unlockTimeouts string
	selectTimeouts string
	deleteRow      string
	unlockRow      string
}

// 用于锁定行行数据
type rowItem struct {
	id    int64
	topic int
}

type timeoutRow struct {
	Id         int64     `db:"id"`
	UpdateTime time.Time `db:"update_time"`
}

// map<topic, listener> 消息处理器map，每一个topic对应一种listener对象
type RowListeners map[int]IRowListener

// db			数据库连接
// tableName 	数据库表名
// listeners    消息处理器map
// args			默认参数，如果不传，则有默认值
func NewMySQLQueue(db *sql.DB, tableName string, listeners RowListeners, args *MySQLQueueArgs) *MySQLQueue {
	if db == nil || tableName == "" || len(listeners) == 0 {
		logger.Error("invalid argument, db=%v, tableName=%q, listeners=%v", db, tableName, listeners)
		return nil
	}

	if args == nil {
		args = &MySQLQueueArgs{}
	}
	args.checkFillDefaultArgs()

	var topicString = fetchTopicString(listeners)
	var concurrency = args.Concurrency
	var mq = &MySQLQueue{
		db:   dbi.NewDB(db),
		args: args,

		selectForLock:  fmt.Sprintf("select id, topic from %s where locked = 0 and topic in (%s) and retry > 0 and now() > update_time limit %d for update;", tableName, topicString, concurrency),
		updateForLock:  fmt.Sprintf("update %s set locked = 1, retry = retry - 1 where id in (%%s);", tableName),
		extendLife:     fmt.Sprintf("update %s set update_time = now() where id = ?;", tableName),
		unlockTimeouts: fmt.Sprintf("update %s set locked = 0 where locked = 1 and topic in (%s) and retry > 0 and now() > date_add(update_time, interval 60 second) limit 128;", tableName, topicString),
		selectTimeouts: fmt.Sprintf("select id, update_time from %s where locked = 1 and topic in (%s) and retry > 0 and now() > date_add(update_time, interval 60 second) limit 128 for update;", tableName, topicString),
		deleteRow:      fmt.Sprintf("delete from %s where id = ?;", tableName),

		// 解锁的时候，利用 update_time 把重试时间设置到 ? seconds 之后
		// 注意：设置了 update_time，不代表一定会重试处理，后者取决于retry字段是否 > 0 。比如retry=3，则nextRetrySeconds()方法最多会被调用3次，
		// 但第3次调用后retry=0，因此虽然设置了下次重试时间，但不会进行重试了。除非手动设置retry字段的值
		unlockRow: fmt.Sprintf("update %s set locked = 0, update_time = date_add(now(), interval ? second) where id = ?;", tableName),
	}

	var rowsChan = make(chan rowItem, concurrency)
	var processingRows = &sync.Map{}
	var retryCountMap = &sync.Map{}

	// 主循环
	loom.Go(func(later loom.Later) {
		mq.goLoop(later, tableName, args, rowsChan, processingRows)
	})

	// concurrency个任务处理协程
	for i := 0; i < concurrency; i++ {
		go mq.goProcess(listeners, rowsChan, processingRows, retryCountMap)
	}

	return mq
}

func (mq *MySQLQueue) goLoop(later loom.Later, tableName string, args *MySQLQueueArgs, rowsChan chan rowItem, processingRows *sync.Map) {
	// 随机sleep一段时间，当有多个MySQLQueue时，避免数据库雪崩
	RandomSleep(0, args.PollInterval)

	var pollTicker = later.NewTicker(args.PollInterval)
	var timeoutTicker = later.NewTicker(args.LockTimeout)
	// 续命的时间间隔，需要小于超时的时间间隔
	var extendLifeTicker = later.NewTicker(args.LockTimeout / 4)

	var concurrency = cap(rowsChan)
	var rowItems = make([]rowItem, 0, concurrency)
	var rowIds = make([]interface{}, 0, concurrency)

	for {
		select {
		case <-pollTicker.C:
			// cap(rowsChan)跟goProcess的数据一样都是concurrency，但是goLoop()调用频率可能会更高一些，而goProcess()的处理速度可能
			// 会非常慢。只所有做这个len(rowsChan)==0的限制，一是为了在处理能力有限的情况下不要锁定太多的行，二是为了不会卡死goLoop()
			// 这个goroutine。否则，如果卡死了很长时间，当rowsChan <- item 恢复执行后， onUnlockTimeouts()有可能先于onExtendLife()
			// 执行，从而导致意外解锁已经加入到了rowsChan中的数据行。
			if len(rowsChan) == 0 {
				// 每次都要重置rowItems与rowIds
				rowItems, err := mq.lockForProcess(rowItems[:0], rowIds[:0])
				if err == nil {
					for i := 0; i < len(rowItems); i++ {
						var item = rowItems[i]
						// 为什么将rowId存储到processingRows的方法提取到这里？原因是在goProcess()的处理过程中，处理时候可能非常长，此时
						// 新一轮的lockForProcess()会加入新的rowItem到rowsChan中，在原来的方案中，由于goProcess()迟迟处理不完，会导致
						// processingRows中不会加入新的rowId。这样，在extendLifeTicker中就不会将这些新的已经加锁的任务续命。进一步导致
						// onUnlockTimeouts()方法会解锁这个新加入的任务。这样，就意味着在多机部署的情况下，可能会有多个进程靠后拿到了对应
						// 的rowId，从而导致同一个rowId被多次处理。
						processingRows.Store(item.id, nil)
						rowsChan <- item
					}
				} else {
					logger.Error(err)
				}
			}
		case <-timeoutTicker.C:
			mq.onUnlockTimeouts()
		case <-extendLifeTicker.C:
			// 正在处理中的rows，每隔一段时间将拿到的锁续命一下，防止被unlockTicker强制解锁
			processingRows.Range(func(key, value interface{}) bool {
				var rowId = key.(int64)
				mq.onExtendLife(rowId)
				return true
			})
		}
	}
}

func (mq *MySQLQueue) goProcess(listeners RowListeners, rowsChan chan rowItem, processingRows *sync.Map, retryCountMap *sync.Map) {
	for {
		select {
		case row := <-rowsChan:
			var listener, ok = listeners[row.topic]
			if !ok {
				logger.Warn("can not find listener for row=%v", row)
				continue
			}

			var rowId = row.id
			//processingRows.Store(rowId, nil)
			var action = safeConsume(listener, rowId)
			processingRows.Delete(rowId)

			switch action {
			case CommitMessage:
				mq.onDeleteRow(rowId, retryCountMap)
			case ReconsumeLater:
				mq.onUnlockRow(rowId, retryCountMap)
			default:
				var message = fmt.Sprintf("invalid action = %d", action)
				panic(message)
			}
		}
	}
}

func safeConsume(listener IRowListener, rowId int64) int {
	defer func() {
		if rec := recover(); rec != nil {
			logger.Error("panic - %v \n%s", rec, debug.Stack())
		}
	}()

	return listener.Consume(rowId)
}

func (mq *MySQLQueue) onDeleteRow(rowId int64, retryCountMap *sync.Map) {
	retryCountMap.Delete(rowId)

	var ctx, cancel = mq.newTimeoutContext()
	defer cancel()

	var result, err = mq.db.ExecContext(ctx, mq.deleteRow, rowId)
	printResult("[onDeleteRow()] rowId="+strconv.FormatInt(rowId, 10), result, err)
}

func (mq *MySQLQueue) onUnlockRow(rowId int64, retryCountMap *sync.Map) {
	var lastRetryCount, _ = retryCountMap.LoadOrStore(rowId, 0)
	var retryCount = lastRetryCount.(int) + 1
	retryCountMap.Store(rowId, retryCount)

	// 计算下一次重试的间隔时间
	var retryInterval = mq.args.RetryInterval(retryCount)
	var retrySeconds = int64(retryInterval / time.Second)

	var ctx, cancel = mq.newTimeoutContext()
	defer cancel()

	var result, err = mq.db.ExecContext(ctx, mq.unlockRow, retrySeconds, rowId)
	printResult("[onUnlockRow()] rowId="+strconv.FormatInt(rowId, 10), result, err)
}

func (mq *MySQLQueue) lockForProcess(rowItems []rowItem, rowIds []interface{}) ([]rowItem, error) {
	var ctx, cancel = mq.newTimeoutContext()
	defer cancel()

	var tx, err = mq.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, mq.selectForLock)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	// 循环获取id和topic
	for rows.Next() {
		var id int64
		var topic int
		if err := rows.Scan(&id, &topic); err != nil {
			return nil, err
		}

		rowItems = append(rowItems, rowItem{id: id, topic: topic})
		rowIds = append(rowIds, id)
	}

	if len(rowItems) == 0 {
		// 走这里会rollback， 逻辑没问题
		return rowItems, nil
	}

	var query = fmt.Sprintf(mq.updateForLock, placeholders(len(rowIds)))
	result, err := tx.ExecContext(ctx, query, rowIds...)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()

	var rowsAffected, err1 = result.RowsAffected()
	logger.Debug("[lockForProcess()] rowItems=%v, rowsAffected=%d, err=%q", rowItems, rowsAffected, err1)
	return rowItems, err
}

func (mq *MySQLQueue) onUnlockTimeouts() {
	var ctx, cancel = mq.newTimeoutContext()
	defer cancel()

	//// 某些row在处理过程，进程会意外重启，因此会处于中间状态。这些row在超时后会被强制解锁，从而有机会重新处理
	//var result, err = mq.db.ExecContext(ctx, mq.unlockTimeouts)
	//printResult("[onUnlockTimeouts()]", result, err)

	var tx, err = mq.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		logger.Info("[onUnlockTimeouts(1)] err = %q", err)
		return
	}
	defer tx.Rollback()

	var rows []timeoutRow
	err = tx.SelectContext(ctx, &rows, mq.selectTimeouts)
	if err != nil {
		logger.Info("[onUnlockTimeouts(2)] err = %q", err)
		return
	}

	result, err := tx.ExecContext(ctx, mq.unlockTimeouts)
	printResult(fmt.Sprintf("[onUnlockTimeouts()] rows=%+v", rows), result, err)
	if err == nil {
		_ = tx.Commit()
	}
}

func (mq *MySQLQueue) onExtendLife(rowId int64) {
	var ctx, cancel = mq.newTimeoutContext()
	defer cancel()

	var result, err = mq.db.ExecContext(ctx, mq.extendLife, rowId)
	printResult("[onExtendLife()] rowId="+strconv.FormatInt(rowId, 10), result, err)
}

func (mq *MySQLQueue) newTimeoutContext() (*dbi.Context, context.CancelFunc) {
	var startTime = time.Now()
	var ctxTimeout, cancel = context.WithTimeout(context.Background(), mq.args.SQLTimeout)
	var ctx = dbi.NewContext(ctxTimeout, dbi.ContextArgs{ErrorFilter: func(err error) error {
		var endTime = time.Now()
		var costTime = endTime.Sub(startTime)
		if costTime > mq.args.SlowSQLWarn {
			logger.Warn("slow sql found, costTime=%v", costTime)
		}

		switch err {
		case nil, sql.ErrNoRows, sql.ErrTxDone:
		default:
			logger.Error("mysql error, err=%q", err)
		}

		return err
	}})

	return ctx, cancel
}

func printResult(title string, result sql.Result, err error) {
	if err != nil {
		logger.Debug("%s err=%q", title, err)
		return
	}

	var rowsAffected, _ = result.RowsAffected()
	logger.Debug("%s rowsAffected=%d", title, rowsAffected)
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

func fetchTopicString(listeners map[int]IRowListener) string {
	var topics = make([]string, 0, len(listeners))
	for i := range listeners {
		var s = strconv.Itoa(i)
		topics = append(topics, s)
	}

	var text = strings.Join(topics, ",")
	return text
}
