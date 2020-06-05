package dbq

import (
	"database/sql"
	"github.com/lixianmin/dbq/logger"
)

/********************************************************************
created:    2020-04-25
author:     lixianmin

Copyright (C) - All Rights Reserved
 *********************************************************************/

func Init(log logger.ILogger) {
	logger.Init(log)
}

func dot(err error) error {
	if err != nil && err != sql.ErrTxDone && err != sql.ErrNoRows {
		logger.GetDefaultLogger().Error("err=%q", err)
	}
	return err
}
