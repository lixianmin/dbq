package dbq

/********************************************************************
created:    2020-04-25
author:     lixianmin

Copyright (C) - All Rights Reserved
 *********************************************************************/

type MySQLQueueArgs struct {
	concurrency      int
	nextRetrySeconds func(retryCount int) int
}

func (args *MySQLQueueArgs) checkFillDefaultArgs() {

	// 协程并发数，最小1，默认10
	if args.concurrency <= 0 {
		args.concurrency = 10
	}

	// retryCount是指第几次重试，比如 nextRetryInterval(1)，意味着已经处理过1次，但是失败了，这是第1次重试
	// 默认的重试间隔为60秒
	if args.nextRetrySeconds == nil {
		args.nextRetrySeconds = func(retryCount int) int {
			return 60
		}
	}
}
