package dbq

import "time"

/********************************************************************
created:    2020-04-25
author:     lixianmin

Copyright (C) - All Rights Reserved
 *********************************************************************/

type MySQLQueueArgs struct {
	Concurrency   int                                // 协程并发数，默认10
	PollInterval  time.Duration                      // 轮询新消息的时间间隔, 默认500ms
	LockTimeout   time.Duration                      // 锁定超时后，将强制解锁，默认2min
	SlowSQLWarn   time.Duration                      // 慢sql报警时间，默认1s
	SQLTimeout    time.Duration                      // sql语句执行超时，默认1min
	RetryInterval func(retryCount int) time.Duration // 下一次重试的间隔时间，默认每次间隔60秒
}

func (args *MySQLQueueArgs) checkFillDefaultArgs() {

	// 协程并发数，最小1，默认10
	if args.Concurrency <= 0 {
		args.Concurrency = 10
	}

	// 轮询新消息的时间间隔, 默认500ms
	if args.PollInterval <= 0 {
		args.PollInterval = 500 * time.Millisecond
	}

	// 锁定超时后，将强制解锁，默认2min
	if args.LockTimeout <= 0 {
		args.LockTimeout = 2 * time.Minute
	}

	if args.SlowSQLWarn <= 0 {
		args.SlowSQLWarn = time.Second
	}

	// sql语句执行超时，默认1min
	if args.SQLTimeout <= 0 {
		args.SQLTimeout = time.Minute
	}

	// retryCount是指第几次重试，比如 RetryInterval(1)，意味着已经处理过1次，但是失败了，这是第1次重试
	// 默认的重试间隔为60秒
	if args.RetryInterval == nil {
		args.RetryInterval = func(retryCount int) time.Duration {
			return 60 * time.Second
		}
	}
}
