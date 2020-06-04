package dbq

import (
	"math/rand"
	"time"
)

/********************************************************************
created:    2020-06-04
author:     lixianmin

Copyright (C) - All Rights Reserved
 *********************************************************************/

func RandomSleep(from, to time.Duration) {
	var delta = to - from
	if delta < 0 {
		panic("delta < 0")
	}

	var d = from + time.Duration(rand.Int63n(int64(delta)))
	time.Sleep(d)
}